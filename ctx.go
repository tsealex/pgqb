package pgqb

import "bytes"

type BuildContextMode uint64

const ContextModeNone = BuildContextMode(0)

const (
	ContextModeNamedArgument BuildContextMode = 1 << iota
)

type buildContextState uint8

const (
	// Column declaration (i.e. during rendering the SELECT clause)
	buildContextStateDeclaration buildContextState = iota
)

type BuildContext struct {
	buf   bytes.Buffer
	mode  BuildContextMode
	state buildContextState

	currArgNum  int
	namedArgNum map[string]int
}

func (ctx *BuildContext) NamedArgumentMode() bool {
	return ctx.mode&ContextModeNamedArgument != ContextModeNone
}

func (ctx *BuildContext) getArgNum(tag string) int {
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

func (ctx *BuildContext) nextArgNum() int {
	ctx.currArgNum++
	return ctx.currArgNum
}

func (ctx *BuildContext) QuoteObject(name string) string {
	return `"` + name + `"`
}
