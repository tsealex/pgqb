package pgqb

import "bytes"

// SQL context.
type Context struct {
	mode ContextMode
}

func (ctx *Context) createBuildContext() *buildContext {
	return newBuildContext(ctx.mode)
}

func (ctx *Context) Select(exps ... interface{}) *SelectStmt {
	return newSelect(ctx, exps...)
}

// Create a context using default mode.
func NewContext() *Context {
	return &Context{mode: ContextModeAutoFrom}
}

//
type ContextMode uint64

const ContextModeNone = ContextMode(0)

const (
	ContextModeNamedArgument ContextMode = 1 << iota
	ContextModeAutoFrom                  = 1 << iota
)

type buildContextState uint8

const (
	buildContextStateNone buildContextState = iota
	// Column declaration (i.e. during rendering the SELECT clause)
	buildContextStateColumnDeclaration = iota
)

type buildContext struct {
	buf   bytes.Buffer
	mode  ContextMode
	state buildContextState

	currArgNum  int
	namedArgNum map[string]int
}

func (ctx *buildContext) NamedArgumentMode() bool {
	return ctx.mode&ContextModeNamedArgument != ContextModeNone
}

// Automatically fill in missing column sources to the FROM clause.
func (ctx *buildContext) AutoFrom() bool {
	return ctx.mode&ContextModeAutoFrom != ContextModeNone
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

func newBuildContext(mode ContextMode) *buildContext {
	return &buildContext{
		buf:         *bytes.NewBuffer([]byte{}),
		mode:        mode,
		state:       buildContextStateNone,
		namedArgNum: map[string]int{},
	}
}

// Table/view/alias name -> ColSource instance
type colSrcMap map[string]ColSource

func (m colSrcMap) Subtract(srcMap colSrcMap) []ColSource {
	var res []ColSource
	for s, colSrc := range m {
		if _, in := srcMap[s]; !in {
			res = append(res, colSrc)
		}
	}
	return res
}
