THRIFT=thrift --gen py:new_style
all: gen-py/data/ gen-py/node/ gen-py/way/ gen-py/relation/

gen-py/data/: data.thrift
	${THRIFT} $<

gen-py/node/: node.thrift
	${THRIFT} $<

gen-py/way/: way.thrift
	${THRIFT} $<

gen-py/relation/: relation.thrift
	${THRIFT} $<
