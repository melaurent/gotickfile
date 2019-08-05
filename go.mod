module github.com/melaurent/gotickfile

go 1.12

require (
	github.com/dsnet/golib/memfile v0.0.0-20190531212259-571cdbcff553
	github.com/satori/go.uuid v1.2.0
	github.com/spf13/afero v1.2.2
)

replace github.com/spf13/afero => ../afero
