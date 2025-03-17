package codec

type UnmarshallerMap[E any] map[string]Unmarshaller[E]

func NewUnmarshallerMap[E any]() UnmarshallerMap[E] {
	return make(UnmarshallerMap[E], 0)
}

func (um UnmarshallerMap[E]) Add(typeHint string, unmarshaller Unmarshaller[E]) UnmarshallerMap[E] {
	um[typeHint] = unmarshaller
	return um
}

func (um UnmarshallerMap[E]) AddFunc(typeHint string, f UnmarshalerFunc[E]) UnmarshallerMap[E] {
	return um.Add(typeHint, f)
}
