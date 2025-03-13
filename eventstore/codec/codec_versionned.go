package codec

type Versioned[E any] struct {
	TypedCodec[E]
}

type VersionSetter interface {
	SetVersion(version string)
}

func (v *Versioned[E]) UnmarshallWithVersion(version string, payload []byte) (event E, err error) {
	event, err = v.TypedCodec.Unmarshall(payload)
	versioned, ok := any(&event).(VersionSetter)
	if ok {
		versioned.SetVersion(version)
	}
	return event, err
}
