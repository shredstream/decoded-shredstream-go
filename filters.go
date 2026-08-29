package decodedshredstream

const (
	maxNamedFilters  = 16
	maxKeysPerFilter = 1000
)

type Filter struct {
	Include  []string
	Exclude  []string
	Required []string
}

func FilterAll() Filter { return Filter{} }

func (f Filter) keyCount() int { return len(f.Include) + len(f.Exclude) + len(f.Required) }

type Filters map[string]Filter

func (m Filters) Validate() error {
	if len(m) == 0 {
		return invalidFilterf("empty filter map delivers nothing — use FilterAll() to receive everything")
	}
	if len(m) > maxNamedFilters {
		return invalidFilterf("%d named filters (max %d)", len(m), maxNamedFilters)
	}
	for name, f := range m {
		if n := f.keyCount(); n > maxKeysPerFilter {
			return invalidFilterf("filter %q has %d keys (max %d per filter)", name, n, maxKeysPerFilter)
		}
		for _, list := range [3][]string{f.Include, f.Exclude, f.Required} {
			for _, key := range list {
				if len(base58Decode(key)) != 32 {
					return invalidFilterf("filter %q: key %q is not a base58-encoded 32-byte public key", name, key)
				}
			}
		}
	}
	return nil
}

func (m Filters) clone() Filters {
	out := make(Filters, len(m))
	for name, f := range m {
		out[name] = Filter{
			Include:  append([]string(nil), f.Include...),
			Exclude:  append([]string(nil), f.Exclude...),
			Required: append([]string(nil), f.Required...),
		}
	}
	return out
}
