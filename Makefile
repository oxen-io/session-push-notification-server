
.PHONY: spns clean
spns:
ifeq (,$(wildcard ./build))
	mkdir -p build
	cmake -B build -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release \
		-DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON \
		-DCMAKE_POLICY_DEFAULT_CMP0069=NEW

endif
	$(MAKE) -C build

clean:
	rm -rf build spns/core.cpython*.so
