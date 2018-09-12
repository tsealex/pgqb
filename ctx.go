package pgqb

import "bytes"

type BuildContextMode uint64

const BuildContextModeNone = BuildContextMode(0)

const (
	ContextModeNamedArgument BuildContextMode = 1 << iota
)

type buildContextState uint8

const (
	buildContextStateNone buildContextState = iota
	// Column declaration (i.e. during rendering the SELECT clause)
	buildContextStateDeclaration buildContextState = iota
)

type buildContext struct {
	buf   bytes.Buffer
	mode  BuildContextMode
	state buildContextState

	currArgNum  int
	namedArgNum map[string]int
}

func (ctx *buildContext) NamedArgumentMode() bool {
	return ctx.mode&ContextModeNamedArgument != BuildContextModeNone
}

func (ctx *buildContext) getArgNum(tag string) int {
	if tag == "" {
		return ctx.nextArgNum()
	}
	var argNum int
	var in bool
	if argNum, in = ctx.namedArgNum[tag]; !in {
		argNum = ctx.nextArgNum()
		ctx.namedArgNum[tag] = argNum
	}
	return argNum
}

func (ctx *buildContext) nextArgNum() int {
	ctx.currArgNum++
	return ctx.currArgNum
}

func (ctx *buildContext) QuoteObject(name string) string {
	return `"` + name + `"`
}

func NewBuildContext(mode BuildContextMode) *buildContext {
	return &buildContext{
		buf: *bytes.NewBuffer([]byte{}),
		mode: mode,
		state: buildContextStateNone,
		namedArgNum: map[string]int{},
	}
}