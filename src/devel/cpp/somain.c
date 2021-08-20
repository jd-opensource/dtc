#undef _GNU_SOURCE
#define _GNU_SOURCE
#include <stdlib.h>
#include <unistd.h>
#include <version.h>
#include <compiler.h>

const char __invoke_dynamic_linker__[]
__attribute__ ((section (".interp")))
__HIDDEN
=
#if __x86_64__
	"/lib64/ld-linux-x86-64.so.2"
#else
	"/lib/ld-linux.so.2"
#endif
	;

__HIDDEN
void _so_start(char *arg1,...) {
#define BANNER "DTC client API v" DTC_VERSION_DETAIL "\n" \
	"  - TCP connection supported\n" \
	"  - UDP connection supported\n" \
	"  - UNIX stream connection supported\n" \
	"  - embeded threading connection supported\n" \
	"  - protocol packet encode/decode interface supported\n" \
	"  - async do_execute (except embeded threading) supported \n"

	int unused = 0;
	if(unused == 0)
		unused = write(1, BANNER, sizeof(BANNER)-1);
	_exit(0);
}

