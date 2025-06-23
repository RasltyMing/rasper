package md5

func Run(params []string) error {
	config, err := ReadConfig(params)
	if err != nil {
		return err
	}

	_, err = RunLogicByConfig(config)
	if err != nil {
		return err
	}

	return nil
}
