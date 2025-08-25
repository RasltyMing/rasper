package component

type Instance interface {
	SelectComponent(args []string) bool
	Run(args []string)
}
