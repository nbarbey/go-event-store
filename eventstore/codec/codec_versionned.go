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

func (v *Versioned[E]) UnmarshallWithTypeAndVersion(version string, typeHint string, payload []byte) (event E, err error) {
	event, err = v.TypedCodec.UnmarshallWithType(typeHint, payload)
	versioned, ok := any(&event).(VersionSetter)
	if ok {
		versioned.SetVersion(version)
	}
	return event, err
}

type VersionedTypedUnmarshaller[E any] interface {
	UnmarshallWithTypeAndVersion(version string, typeHint string, payload []byte) (event E, err error)
}

func UnmarshallAllWithTypeAndVersions[E any](u VersionedTypedUnmarshaller[E], types []string, versions []string, payloads [][]byte) (events []E, err error) {
	output := make([]E, 0)
	for i, payload := range payloads {
		event, err := u.UnmarshallWithTypeAndVersion(versions[i], types[i], payload)
		if err != nil {
			return nil, err
		}
		output = append(output, event)
	}
	return output, nil
}
