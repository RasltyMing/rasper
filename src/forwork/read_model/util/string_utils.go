package util

func StringReturnNil(str *string, equal string) *string {
	if *str == equal {
		return nil
	}

	return str
}

func SafeGetString(strPtr *string) string {
	if strPtr == nil {
		return ""
	}
	return *strPtr
}
