exception UnknownWay {
	1: i64 id
}

exception UnknownVersion {
	1: i32 version,
	2: optional i64 node,
	3: optional i64 way,
	4: optional i64 relation
}
