package labgob

//
// 尝试通过 RPC 发送非大写字段会产生一系列
// 不当行为，包括神秘的错误计算和
// 彻底崩溃。 所以这个围绕 Go 的编码/gob 的包装器发出警告
// 关于非大写字段名称。
//
import "encoding/gob"
import "io"
import "reflect"
import "fmt"
import "sync"
import "unicode"
import "unicode/utf8"

var mu sync.Mutex
var errorCount int // for TestCapital
var checked map[reflect.Type]bool

type LabEncoder struct {
	gob *gob.Encoder
}

func NewEncoder(w io.Writer) *LabEncoder {
	enc := &LabEncoder{}
	enc.gob = gob.NewEncoder(w)
	return enc
}

func (enc *LabEncoder) Encode(e interface{}) error {
	checkValue(e)
	return enc.gob.Encode(e)
}

func (enc *LabEncoder) EncodeValue(value reflect.Value) error {
	checkValue(value.Interface())
	return enc.gob.EncodeValue(value)
}

type LabDecoder struct {
	gob *gob.Decoder
}

func NewDecoder(r io.Reader) *LabDecoder {
	dec := &LabDecoder{}
	dec.gob = gob.NewDecoder(r)
	return dec
}

func (dec *LabDecoder) Decode(e interface{}) error {
	checkValue(e)
	checkDefault(e)
	return dec.gob.Decode(e)
}

func Register(value interface{}) {
	checkValue(value)
	gob.Register(value)
}

func RegisterName(name string, value interface{}) {
	checkValue(value)
	gob.RegisterName(name, value)
}

func checkValue(value interface{}) {
	checkType(reflect.TypeOf(value))
}

func checkType(t reflect.Type) {
	k := t.Kind()

	mu.Lock()
	// only complain once, and avoid recursion.
	if checked == nil {
		checked = map[reflect.Type]bool{}
	}
	if checked[t] {
		mu.Unlock()
		return
	}
	checked[t] = true
	mu.Unlock()

	switch k {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			rune, _ := utf8.DecodeRuneInString(f.Name)
			if unicode.IsUpper(rune) == false {
				// ta da
				fmt.Printf("labgob error: lower-case field %v of %v in RPC or persist/snapshot will break your Raft\n",
					f.Name, t.Name())
				mu.Lock()
				errorCount += 1
				mu.Unlock()
			}
			checkType(f.Type)
		}
		return
	case reflect.Slice, reflect.Array, reflect.Ptr:
		checkType(t.Elem())
		return
	case reflect.Map:
		checkType(t.Elem())
		checkType(t.Key())
		return
	default:
		return
	}
}

// 如果该值包含非默认值，则发出警告，
// 就像发送 RPC 但回复一样
// 结构已经被修改。 如果 RPC 回复
// 包含默认值，GOB 不会覆盖
// 非默认值。
func checkDefault(value interface{}) {
	if value == nil {
		return
	}
	checkDefault1(reflect.ValueOf(value), 1, "")
}

func checkDefault1(value reflect.Value, depth int, name string) {
	if depth > 3 {
		return
	}

	t := value.Type()
	k := t.Kind()

	switch k {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			vv := value.Field(i)
			name1 := t.Field(i).Name
			if name != "" {
				name1 = name + "." + name1
			}
			checkDefault1(vv, depth+1, name1)
		}
		return
	case reflect.Ptr:
		if value.IsNil() {
			return
		}
		checkDefault1(value.Elem(), depth+1, name)
		return
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Uintptr, reflect.Float32, reflect.Float64,
		reflect.String:
		if reflect.DeepEqual(reflect.Zero(t).Interface(), value.Interface()) == false {
			mu.Lock()
			if errorCount < 1 {
				what := name
				if what == "" {
					what = t.Name()
				}
				// 如果代码重复使用相同的 RPC 回复，通常会出现此警告
				// 多个 RPC 调用的变量，或者如果代码恢复持久化
				// 状态到已经有非默认值的变量。
				fmt.Printf("labgob warning: Decoding into a non-default variable/field %v may not work\n",
					what)
			}
			errorCount += 1
			mu.Unlock()
		}
		return
	}
}
