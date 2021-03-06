SRCDIR=$(NEDGE_HOME)

ifdef NEDGE_NDEBUG
DEBUG_FLAGS=-DUSE_JE_MALLOC
DEBUG_LDFLAGS=-ljemalloc -fno-omit-frame-pointer
else
DEBUG_FLAGS=-fsanitize=address -fno-omit-frame-pointer -fno-common

ifeq (${CC},clang)
DEBUG_LDFLAGS=-fsanitize=address
else
DEBUG_LDFLAGS=-lasan
endif

endif

TARGET := grpc-s3x
SRCS := main.go $(wildcard */*.go)
GOPATH?=$(SRCDIR)/src

.PHONY: all clean

.get:
	GOPATH=$(GOPATH) flock -e $(SRCDIR)/.lock go get -v || true
	GOPATH=$(GOPATH) flock -e $(SRCDIR)/.lock go get -u github.com/golang/protobuf/proto || true
	GOPATH=$(GOPATH) flock -e $(SRCDIR)/.lock go get -u github.com/golang/protobuf/protoc-gen-go || true
	GOPATH=$(GOPATH) flock -e $(SRCDIR)/.lock go get google.golang.org/grpc || true
	GOPATH=$(GOPATH) flock -e $(SRCDIR)/.lock go get github.com/fullstorydev/grpcurl || true
	GOPATH=$(GOPATH) flock -e $(SRCDIR)/.lock go get github.com/pborman/getopt || true
	GOPATH=$(GOPATH) flock -e $(SRCDIR)/.lock go install github.com/fullstorydev/grpcurl/cmd/grpcurl || true
	flock -e $(SRCDIR)/.lock ../../scripts/get-protobuf.sh
	touch $@

.proto: .get $(wildcard */*.proto)
	for d in status; do \
		/tmp/protoc/bin/protoc --proto_path=./$$d --plugin=$(GOPATH)/bin/protoc-gen-go --go_out=plugins=grpc:$$d ./$$d/$$d.proto; \
	done
	touch $@

$(TARGET): $(SRCS) .get .proto
	GOPATH=$(GOPATH) CGO_LDFLAGS="-L$(SRCDIR)/lib -lccow -lccowutil -lnanomsg $(DEBUG_LDFLAGS)" \
		CGO_CFLAGS="$(DEBUG_FLAGS) -I$(SRCDIR)/include/ccow -I$(SRCDIR)/include" \
		go build -o grpc-s3x main.go

fmt:
	gofmt -e -s -w status main.go

clean:
	rm -rf $(TARGET) src .get $(wildcard */*.pb.go) /tmp/protoc

install: $(TARGET)
	mkdir -p $(DESTDIR)$(SRCDIR)/sbin 2> /dev/null || true
	cp -a $(TARGET) $(DESTDIR)$(SRCDIR)/sbin
	mkdir -p $(DESTDIR)$(SRCDIR)/etc/ssl 2> /dev/null || true
	cp -a default.key $(DESTDIR)$(SRCDIR)/etc/ssl/ssl.key
	cp -a default.crt $(DESTDIR)$(SRCDIR)/etc/ssl/ssl.crt

all: install

uninstall:
	rm -f $(DESTDIR)$(SRCDIR)/sbin/$(TARGET)
