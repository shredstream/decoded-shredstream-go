package decodedshredstream

const b58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

var b58Index = func() [256]int8 {
	var idx [256]int8
	for i := range idx {
		idx[i] = -1
	}
	for i := 0; i < len(b58Alphabet); i++ {
		idx[b58Alphabet[i]] = int8(i)
	}
	return idx
}()

func base58Encode(b []byte) string {
	zeros := 0
	for zeros < len(b) && b[zeros] == 0 {
		zeros++
	}
	size := (len(b)-zeros)*138/100 + 1
	buf := make([]byte, size)
	high := size - 1
	for _, c := range b[zeros:] {
		carry := int(c)
		i := size - 1
		for ; i > high || carry != 0; i-- {
			carry += 256 * int(buf[i])
			buf[i] = byte(carry % 58)
			carry /= 58
		}
		high = i
	}
	start := 0
	for start < size && buf[start] == 0 {
		start++
	}
	out := make([]byte, zeros+size-start)
	for i := 0; i < zeros; i++ {
		out[i] = '1'
	}
	for i, v := range buf[start:] {
		out[zeros+i] = b58Alphabet[v]
	}
	return string(out)
}

func base58Decode(s string) []byte {
	if s == "" {
		return []byte{}
	}
	zeros := 0
	for zeros < len(s) && s[zeros] == '1' {
		zeros++
	}
	size := (len(s)-zeros)*733/1000 + 1 // log(58)/log(256) ≈ 0.733
	buf := make([]byte, size)
	high := size - 1
	for k := zeros; k < len(s); k++ {
		d := b58Index[s[k]]
		if d < 0 {
			return nil
		}
		carry := int(d)
		i := size - 1
		for ; i > high || carry != 0; i-- {
			carry += 58 * int(buf[i])
			buf[i] = byte(carry % 256)
			carry /= 256
		}
		high = i
	}
	start := 0
	for start < size && buf[start] == 0 {
		start++
	}
	out := make([]byte, zeros+size-start)
	copy(out[zeros:], buf[start:])
	return out
}
