@echo Installing tools...

pushd tools
call install.cmd
popd

@echo Building HTML UI...

pushd htmlui
call build.cmd
popd

@echo Building embedded data...

pushd htmlui\build
echo on
..\..\tools\.tools\go-bindata.exe -fs -tags embedhtml -o ..\..\internal\server\htmlui_bindata.go -pkg server -ignore .map . static/css static/js static/media
popd

@echo Building binary...
go build -o kopia.exe -tags embedhtml github.com/kopia/kopia
