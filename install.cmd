@echo Building HTML UI...

pushd htmlui
call build.cmd
popd

@echo Building binary...
go build -o kopia.exe -tags embedhtml github.com/kopia/kopia
