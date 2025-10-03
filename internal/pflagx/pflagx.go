// Package pflagx implements extensions to pflag.
package pflagx

import (
	"github.com/spf13/pflag"
)

type FlagSet pflag.FlagSet

func FlagSetExt(fs *pflag.FlagSet) *FlagSet {
	return (*FlagSet)(fs)
}

func (fs *FlagSet) FlagSet() *pflag.FlagSet {
	return (*pflag.FlagSet)(fs)
}
