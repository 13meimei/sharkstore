//line sql.y:20
package sqlparser

import __yyfmt__ "fmt"

//line sql.y:20
import "bytes"

func SetParseTree(yylex interface{}, stmt Statement) {
	yylex.(*Tokenizer).ParseTree = stmt
}

func SetAllowComments(yylex interface{}, allow bool) {
	yylex.(*Tokenizer).AllowComments = allow
}

func ForceEOF(yylex interface{}) {
	yylex.(*Tokenizer).ForceEOF = true
}

var (
	SHARE        = []byte("share")
	MODE         = []byte("mode")
	IF_BYTES     = []byte("if")
	VALUES_BYTES = []byte("values")
)

//line sql.y:45
type yySymType struct {
	yys         int
	empty       struct{}
	statement   Statement
	selStmt     SelectStatement
	byt         byte
	bytes       []byte
	bytes2      [][]byte
	str         string
	selectExprs SelectExprs
	selectExpr  SelectExpr
	columns     Columns
	colName     *ColName
	tableExprs  TableExprs
	tableExpr   TableExpr
	smTableExpr SimpleTableExpr
	tableName   *TableName
	indexHints  *IndexHints
	expr        Expr
	boolExpr    BoolExpr
	valExpr     ValExpr
	tuple       Tuple
	valExprs    ValExprs
	values      Values
	subquery    *Subquery
	caseExpr    *CaseExpr
	whens       []*When
	when        *When
	orderBy     OrderBy
	order       *Order
	limit       *Limit
	insRows     InsertRows
	updateExprs UpdateExprs
	updateExpr  *UpdateExpr
}

const LEX_ERROR = 57346
const SELECT = 57347
const INSERT = 57348
const UPDATE = 57349
const DELETE = 57350
const FROM = 57351
const WHERE = 57352
const GROUP = 57353
const HAVING = 57354
const ORDER = 57355
const BY = 57356
const LIMIT = 57357
const FOR = 57358
const ALL = 57359
const DISTINCT = 57360
const AS = 57361
const EXISTS = 57362
const NULL = 57363
const ASC = 57364
const DESC = 57365
const VALUES = 57366
const INTO = 57367
const DUPLICATE = 57368
const KEY = 57369
const DEFAULT = 57370
const SET = 57371
const LOCK = 57372
const GLOBAL = 57373
const SESSION = 57374
const LEVEL = 57375
const ID = 57376
const STRING = 57377
const NUMBER = 57378
const VALUE_ARG = 57379
const COMMENT = 57380
const UNION = 57381
const MINUS = 57382
const EXCEPT = 57383
const INTERSECT = 57384
const JOIN = 57385
const STRAIGHT_JOIN = 57386
const LEFT = 57387
const RIGHT = 57388
const INNER = 57389
const OUTER = 57390
const CROSS = 57391
const NATURAL = 57392
const USE = 57393
const FORCE = 57394
const ON = 57395
const OR = 57396
const AND = 57397
const NOT = 57398
const BETWEEN = 57399
const CASE = 57400
const WHEN = 57401
const THEN = 57402
const ELSE = 57403
const LE = 57404
const GE = 57405
const NE = 57406
const NULL_SAFE_EQUAL = 57407
const IS = 57408
const LIKE = 57409
const IN = 57410
const UNARY = 57411
const END = 57412
const BEGIN = 57413
const START = 57414
const TRANSACTION = 57415
const COMMIT = 57416
const ROLLBACK = 57417
const ISOLATION = 57418
const READ = 57419
const WRITE = 57420
const ONLY = 57421
const REPEATABLE = 57422
const COMMITTED = 57423
const UNCOMMITTED = 57424
const SERIALIZABLE = 57425
const NAMES = 57426
const REPLACE = 57427
const ADMIN = 57428
const OFFSET = 57429
const COLLATE = 57430
const CREATE = 57431
const ALTER = 57432
const DROP = 57433
const RENAME = 57434
const TABLE = 57435
const INDEX = 57436
const VIEW = 57437
const TO = 57438
const IGNORE = 57439
const IF = 57440
const UNIQUE = 57441
const USING = 57442
const TRUNCATE = 57443
const DESCRIBE = 57444

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"LEX_ERROR",
	"SELECT",
	"INSERT",
	"UPDATE",
	"DELETE",
	"FROM",
	"WHERE",
	"GROUP",
	"HAVING",
	"ORDER",
	"BY",
	"LIMIT",
	"FOR",
	"ALL",
	"DISTINCT",
	"AS",
	"EXISTS",
	"NULL",
	"ASC",
	"DESC",
	"VALUES",
	"INTO",
	"DUPLICATE",
	"KEY",
	"DEFAULT",
	"SET",
	"LOCK",
	"GLOBAL",
	"SESSION",
	"LEVEL",
	"ID",
	"STRING",
	"NUMBER",
	"VALUE_ARG",
	"COMMENT",
	"'('",
	"'~'",
	"UNION",
	"MINUS",
	"EXCEPT",
	"INTERSECT",
	"','",
	"JOIN",
	"STRAIGHT_JOIN",
	"LEFT",
	"RIGHT",
	"INNER",
	"OUTER",
	"CROSS",
	"NATURAL",
	"USE",
	"FORCE",
	"ON",
	"OR",
	"AND",
	"NOT",
	"BETWEEN",
	"CASE",
	"WHEN",
	"THEN",
	"ELSE",
	"'='",
	"'<'",
	"'>'",
	"LE",
	"GE",
	"NE",
	"NULL_SAFE_EQUAL",
	"IS",
	"LIKE",
	"IN",
	"'|'",
	"'&'",
	"'+'",
	"'-'",
	"'*'",
	"'/'",
	"'%'",
	"'^'",
	"'.'",
	"UNARY",
	"END",
	"BEGIN",
	"START",
	"TRANSACTION",
	"COMMIT",
	"ROLLBACK",
	"ISOLATION",
	"READ",
	"WRITE",
	"ONLY",
	"REPEATABLE",
	"COMMITTED",
	"UNCOMMITTED",
	"SERIALIZABLE",
	"NAMES",
	"REPLACE",
	"ADMIN",
	"OFFSET",
	"COLLATE",
	"CREATE",
	"ALTER",
	"DROP",
	"RENAME",
	"TABLE",
	"INDEX",
	"VIEW",
	"TO",
	"IGNORE",
	"IF",
	"UNIQUE",
	"USING",
	"TRUNCATE",
	"DESCRIBE",
	"')'",
}
var yyStatenames = [...]string{}

const yyEofCode = 1
const yyErrCode = 2
const yyInitialStackSize = 16

//line yacctab:1
var yyExca = [...]int{
	-1, 1,
	1, -1,
	-2, 0,
}

const yyPrivate = 57344

const yyLast = 690

var yyAct = [...]int{

	115, 161, 112, 413, 381, 195, 283, 193, 149, 78,
	106, 376, 123, 196, 3, 298, 241, 278, 142, 422,
	208, 101, 113, 235, 80, 75, 118, 122, 422, 102,
	128, 39, 40, 41, 42, 224, 65, 170, 169, 94,
	105, 119, 120, 121, 422, 110, 126, 87, 56, 82,
	57, 81, 69, 275, 89, 118, 122, 91, 51, 128,
	53, 95, 394, 57, 54, 109, 163, 129, 297, 105,
	119, 120, 121, 152, 110, 126, 349, 163, 163, 107,
	393, 271, 392, 124, 125, 103, 238, 137, 88, 148,
	100, 83, 424, 90, 109, 58, 129, 156, 269, 341,
	151, 423, 85, 134, 84, 166, 362, 364, 272, 59,
	60, 61, 124, 125, 103, 342, 343, 421, 197, 127,
	192, 194, 198, 157, 141, 160, 291, 62, 373, 290,
	319, 201, 292, 82, 287, 81, 82, 204, 81, 372,
	179, 215, 206, 219, 220, 168, 212, 213, 127, 348,
	330, 328, 140, 273, 270, 221, 79, 205, 133, 239,
	210, 143, 144, 256, 363, 234, 427, 139, 248, 215,
	279, 107, 247, 93, 147, 245, 255, 254, 216, 252,
	250, 251, 257, 258, 169, 261, 262, 263, 264, 265,
	266, 267, 268, 246, 370, 253, 308, 309, 310, 311,
	312, 389, 313, 314, 279, 377, 333, 107, 107, 230,
	145, 178, 177, 180, 181, 182, 183, 184, 179, 286,
	285, 274, 276, 249, 228, 294, 293, 282, 155, 280,
	231, 259, 170, 169, 295, 96, 288, 82, 82, 81,
	303, 182, 183, 184, 179, 391, 301, 135, 135, 300,
	180, 181, 182, 183, 184, 179, 245, 356, 377, 390,
	318, 305, 357, 323, 324, 170, 169, 354, 366, 260,
	360, 375, 355, 359, 320, 55, 358, 322, 209, 327,
	162, 271, 82, 107, 81, 338, 164, 398, 340, 337,
	334, 336, 209, 384, 300, 332, 335, 329, 217, 409,
	408, 227, 229, 226, 178, 177, 180, 181, 182, 183,
	184, 179, 407, 306, 400, 401, 163, 20, 138, 245,
	245, 74, 347, 352, 353, 244, 202, 135, 368, 369,
	243, 321, 281, 371, 39, 40, 41, 42, 200, 379,
	199, 374, 237, 98, 236, 130, 244, 382, 378, 304,
	82, 243, 385, 383, 237, 20, 21, 22, 23, 178,
	177, 180, 181, 182, 183, 184, 179, 178, 177, 180,
	181, 182, 183, 184, 179, 317, 167, 159, 395, 24,
	66, 83, 367, 396, 131, 365, 345, 211, 344, 233,
	316, 66, 232, 403, 405, 216, 212, 207, 404, 76,
	406, 153, 150, 411, 35, 412, 382, 402, 414, 414,
	414, 146, 415, 416, 92, 68, 64, 218, 82, 419,
	81, 49, 50, 428, 132, 410, 397, 425, 429, 20,
	430, 97, 326, 420, 20, 222, 29, 30, 72, 31,
	32, 154, 70, 164, 118, 122, 388, 339, 128, 284,
	33, 34, 387, 299, 25, 26, 28, 27, 83, 119,
	120, 121, 351, 110, 126, 209, 36, 37, 77, 426,
	417, 20, 118, 122, 44, 19, 128, 18, 17, 16,
	15, 14, 13, 109, 12, 129, 83, 119, 120, 121,
	289, 110, 126, 178, 177, 180, 181, 182, 183, 184,
	179, 124, 125, 122, 99, 223, 128, 52, 296, 225,
	86, 109, 302, 129, 418, 20, 83, 119, 120, 121,
	399, 138, 126, 380, 386, 350, 331, 203, 277, 124,
	125, 122, 117, 114, 128, 116, 346, 127, 214, 111,
	171, 108, 361, 129, 83, 119, 120, 121, 242, 138,
	126, 307, 240, 104, 122, 315, 165, 128, 71, 124,
	125, 136, 38, 122, 158, 127, 128, 83, 119, 120,
	121, 129, 138, 126, 73, 11, 83, 119, 120, 121,
	10, 138, 126, 9, 8, 7, 6, 124, 125, 5,
	4, 2, 1, 0, 129, 127, 177, 180, 181, 182,
	183, 184, 179, 129, 0, 0, 0, 0, 0, 0,
	124, 125, 0, 0, 0, 0, 0, 0, 0, 124,
	125, 173, 175, 127, 0, 0, 0, 185, 186, 187,
	188, 189, 190, 191, 176, 174, 172, 178, 177, 180,
	181, 182, 183, 184, 179, 325, 127, 308, 309, 310,
	311, 312, 43, 313, 314, 127, 0, 0, 0, 0,
	0, 0, 178, 177, 180, 181, 182, 183, 184, 179,
	0, 0, 0, 0, 45, 46, 47, 48, 0, 0,
	0, 0, 0, 0, 0, 0, 63, 0, 0, 67,
}
var yyPact = [...]int{

	350, -1000, -1000, 293, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 390, -50, -62, -13, 1, -1000,
	39, -1000, -1000, -1000, 382, 346, -1000, 381, 466, 425,
	-1000, -1000, -1000, 420, -1000, -49, 365, 459, 57, 16,
	14, -66, -21, 346, -1000, -15, 346, -1000, 380, -74,
	346, -74, -1000, 406, 304, -1000, -1000, -18, -1000, -1000,
	-1000, 6, -1000, 307, 359, 395, 75, 365, 203, 533,
	-1000, 102, -1000, 69, 70, 70, 377, 115, 346, -1000,
	368, -1000, -38, 367, 421, 172, 346, 365, 342, 365,
	-1000, 271, -1000, -1000, 357, 62, 175, 562, -1000, 452,
	424, -1000, -1000, -1000, 542, 301, 299, -1000, 287, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 542,
	-1000, 365, 347, 363, 455, 347, -1000, 284, 510, 482,
	361, 253, -1000, 384, 50, 253, -1000, 415, -80, -1000,
	196, -1000, 358, -1000, -1000, 355, -1000, 315, 41, -1000,
	-1000, -1000, 291, 6, 542, -1000, -1000, 346, 144, 452,
	452, 542, 279, 103, 542, 542, 210, 542, 542, 542,
	542, 542, 542, 542, 542, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, 562, -20, 36, -10, 562, -1000, 35,
	6, -1000, 466, 108, 418, 303, 282, -1000, 436, 452,
	-1000, 542, 418, 418, -1000, -1000, 51, 70, 34, -1000,
	-1000, -1000, -1000, 170, 346, -1000, -43, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 429, 347, 347, 314, -1000,
	268, 601, 356, 312, 47, -1000, -1000, 229, -1000, -1000,
	-1000, 126, 418, -1000, 279, 542, 542, 418, 587, -1000,
	411, 173, 520, -1000, 162, 162, 58, 58, 58, -1000,
	-1000, 542, -1000, -1000, 33, 6, 32, 142, -1000, 452,
	429, 347, 436, 428, 433, 175, 418, 346, -1000, -1000,
	7, 19, -1000, 354, -1000, -1000, 352, -1000, -1000, 279,
	293, 203, 31, -1000, -1000, 451, 291, 291, -1000, -1000,
	221, 211, 230, 227, 224, 52, -1000, 351, 150, 348,
	542, 542, -1000, 418, 136, 542, -1000, 418, -1000, 21,
	-1000, 43, -1000, 542, 208, 149, 202, 428, -1000, 542,
	-1000, -1000, -1000, -1000, -1000, -1000, 248, -1000, -1000, 347,
	440, 432, 601, 145, -1000, 213, -1000, 199, -1000, -1000,
	-1000, -1000, -27, -29, -47, -1000, -1000, -1000, 418, 418,
	542, 418, -1000, -1000, 418, 542, -1000, 400, -1000, -1000,
	242, -1000, 292, -1000, 279, -1000, 436, 452, 542, 452,
	-1000, -1000, 273, 261, 260, 418, 418, 398, 542, -1000,
	-1000, -1000, -1000, 428, 175, 236, 175, 346, 346, 346,
	463, -1000, 403, -1, -1000, -17, -26, 347, -1000, 462,
	92, -1000, 346, -1000, -1000, 203, -1000, 346, -1000, 346,
	-1000,
}
var yyPgo = [...]int{

	0, 592, 591, 13, 590, 589, 586, 585, 584, 583,
	580, 575, 652, 574, 564, 562, 558, 275, 21, 29,
	556, 555, 553, 552, 16, 551, 548, 25, 542, 3,
	20, 10, 541, 540, 15, 539, 7, 22, 5, 536,
	535, 12, 533, 2, 532, 528, 17, 527, 526, 525,
	524, 6, 523, 4, 520, 1, 514, 23, 512, 11,
	9, 24, 173, 510, 509, 508, 507, 505, 0, 8,
	504, 18, 124, 490, 484, 482, 481, 480, 479, 478,
	477, 475, 474,
}
var yyR1 = [...]int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 3,
	3, 3, 4, 4, 77, 77, 5, 6, 7, 7,
	7, 7, 7, 7, 72, 72, 71, 71, 71, 73,
	73, 73, 73, 14, 14, 14, 74, 74, 75, 76,
	78, 81, 79, 80, 8, 8, 8, 9, 9, 9,
	10, 11, 11, 11, 82, 12, 13, 13, 15, 15,
	15, 15, 15, 16, 16, 18, 18, 19, 19, 19,
	22, 22, 20, 20, 20, 23, 23, 24, 24, 24,
	24, 21, 21, 21, 25, 25, 25, 25, 25, 25,
	25, 25, 25, 26, 26, 26, 27, 27, 28, 28,
	28, 28, 29, 29, 30, 30, 31, 31, 31, 31,
	31, 32, 32, 32, 32, 32, 32, 32, 32, 32,
	32, 33, 33, 33, 33, 33, 33, 33, 34, 34,
	39, 39, 37, 37, 41, 38, 38, 36, 36, 36,
	36, 36, 36, 36, 36, 36, 36, 36, 36, 36,
	36, 36, 36, 36, 40, 40, 42, 42, 42, 44,
	47, 47, 45, 45, 46, 48, 48, 43, 43, 43,
	35, 35, 35, 35, 49, 49, 50, 50, 51, 51,
	52, 52, 53, 54, 54, 54, 55, 55, 55, 55,
	56, 56, 56, 57, 57, 58, 58, 59, 59, 60,
	60, 61, 61, 62, 62, 63, 63, 17, 17, 64,
	64, 64, 64, 64, 65, 65, 66, 66, 67, 67,
	68, 69, 70, 70,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 5,
	12, 3, 8, 8, 6, 6, 8, 7, 3, 4,
	4, 6, 4, 4, 1, 3, 3, 2, 2, 2,
	2, 2, 1, 0, 1, 3, 1, 2, 1, 1,
	5, 2, 2, 4, 5, 8, 4, 6, 7, 4,
	5, 4, 5, 5, 0, 2, 0, 2, 1, 2,
	1, 1, 1, 0, 1, 1, 3, 1, 2, 3,
	1, 1, 0, 1, 2, 1, 3, 3, 3, 3,
	5, 0, 1, 2, 1, 1, 2, 3, 2, 3,
	2, 2, 2, 1, 3, 1, 1, 3, 0, 5,
	5, 5, 1, 3, 0, 2, 1, 3, 3, 2,
	3, 3, 3, 4, 3, 4, 5, 6, 3, 4,
	2, 1, 1, 1, 1, 1, 1, 1, 2, 1,
	1, 3, 3, 1, 3, 1, 3, 1, 1, 1,
	3, 3, 3, 3, 3, 3, 3, 3, 2, 3,
	4, 5, 4, 1, 1, 1, 1, 1, 1, 5,
	0, 1, 1, 2, 4, 0, 2, 1, 3, 5,
	1, 1, 1, 1, 0, 3, 0, 2, 0, 3,
	1, 3, 2, 0, 1, 1, 0, 2, 4, 4,
	0, 2, 4, 0, 3, 1, 3, 0, 5, 1,
	3, 3, 3, 0, 2, 0, 3, 0, 1, 1,
	1, 1, 1, 1, 0, 1, 0, 1, 0, 2,
	1, 0, 0, 1,
}
var yyChk = [...]int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, -74, -75, -76, -77, -78, -79, -80, -81,
	5, 6, 7, 8, 29, 104, 105, 107, 106, 86,
	87, 89, 90, 100, 101, 54, 116, 117, -15, 41,
	42, 43, 44, -12, -82, -12, -12, -12, -12, 31,
	32, 108, -66, 110, 114, -17, 110, 112, 108, 108,
	109, 110, 88, -12, 34, -68, 34, -12, 34, -3,
	17, -16, 18, -13, -17, -27, 34, 9, -60, 99,
	-61, -43, -68, 34, 88, 88, -63, 113, 109, -68,
	108, -68, 34, -62, 113, -68, -62, 25, 39, -70,
	108, -18, -19, 79, -22, 34, -31, -36, -32, 59,
	39, -35, -43, -37, -42, -68, -40, -44, 20, 35,
	36, 37, 21, -41, 77, 78, 40, 113, 24, 61,
	38, 25, 29, 83, -27, 45, 28, -36, 39, 65,
	83, -72, -71, 91, 92, -72, 34, 59, -68, -69,
	34, -69, 111, 34, 20, 56, -68, -27, -14, 35,
	-27, -55, 9, 45, 15, -20, -68, 19, 83, 58,
	57, -33, 74, 59, 73, 60, 72, 76, 75, 82,
	77, 78, 79, 80, 81, 65, 66, 67, 68, 69,
	70, 71, -31, -36, -31, -38, -3, -36, -36, 39,
	39, -41, 39, -47, -36, -27, -60, 34, -30, 10,
	-61, 103, -36, -36, 56, -68, 34, 45, 33, 93,
	94, -69, 20, -67, 115, -64, 107, 105, 28, 106,
	13, 34, 34, 34, -69, -57, 29, 39, 45, 118,
	-23, -24, -26, 39, 34, -41, -19, -36, -68, 79,
	-31, -31, -36, -37, 74, 73, 60, -36, -36, 21,
	59, -36, -36, -36, -36, -36, -36, -36, -36, 118,
	118, 45, 118, 118, -18, 18, -18, -45, -46, 62,
	-57, 29, -30, -51, 13, -31, -36, 83, -71, -73,
	95, 92, 98, 56, -68, -69, -65, 111, -34, 24,
	-3, -60, -58, -43, 35, -30, 45, -25, 46, 47,
	48, 49, 50, 52, 53, -21, 34, 19, -24, 83,
	45, 102, -37, -36, -36, 58, 21, -36, 118, -18,
	118, -48, -46, 64, -31, -34, -60, -51, -55, 14,
	-68, 92, 96, 97, 34, 34, -39, -37, 118, 45,
	-49, 11, -24, -24, 46, 51, 46, 51, 46, 46,
	46, -28, 54, 112, 55, 34, 118, 34, -36, -36,
	58, -36, 118, 85, -36, 63, -59, 56, -59, -55,
	-52, -53, -36, -69, 45, -43, -50, 12, 14, 56,
	46, 46, 109, 109, 109, -36, -36, 26, 45, -54,
	22, 23, -37, -51, -31, -38, -31, 39, 39, 39,
	27, -53, -55, -29, -68, -29, -29, 7, -56, 16,
	30, 118, 45, 118, 118, -60, 7, 74, -68, -68,
	-68,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
	64, 64, 64, 64, 64, 226, 217, 0, 0, 46,
	0, 48, 49, 64, 0, 0, 64, 0, 0, 68,
	70, 71, 72, 73, 66, 217, 0, 0, 0, 0,
	0, 215, 0, 0, 227, 0, 0, 218, 0, 213,
	0, 213, 47, 0, 0, 52, 230, 232, 51, 21,
	69, 0, 74, 65, 0, 0, 106, 0, 28, 0,
	209, 0, 177, 230, 0, 0, 0, 0, 0, 231,
	0, 231, 0, 0, 0, 0, 0, 0, 43, 0,
	233, 196, 75, 77, 82, 230, 80, 81, 116, 0,
	0, 147, 148, 149, 0, 177, 0, 163, 0, 180,
	181, 182, 183, 143, 166, 167, 168, 164, 165, 170,
	67, 0, 0, 0, 114, 0, 29, 30, 0, 0,
	0, 32, 34, 0, 0, 33, 231, 0, 228, 56,
	0, 59, 0, 61, 214, 0, 231, 203, 0, 44,
	53, 19, 0, 0, 0, 78, 83, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 131, 132, 133, 134, 135,
	136, 137, 119, 0, 0, 0, 0, 145, 158, 0,
	0, 130, 0, 0, 171, 203, 114, 107, 188, 0,
	210, 0, 145, 211, 212, 178, 230, 0, 0, 37,
	38, 54, 216, 0, 0, 231, 224, 219, 220, 221,
	222, 223, 60, 62, 63, 0, 0, 0, 0, 50,
	114, 85, 91, 0, 103, 105, 76, 197, 84, 79,
	117, 118, 121, 122, 0, 0, 0, 124, 0, 128,
	0, 150, 151, 152, 153, 154, 155, 156, 157, 120,
	142, 0, 144, 159, 0, 0, 0, 175, 172, 0,
	0, 0, 188, 196, 0, 115, 31, 0, 35, 36,
	0, 0, 42, 0, 229, 57, 0, 225, 24, 0,
	139, 25, 0, 205, 45, 184, 0, 0, 94, 95,
	0, 0, 0, 0, 0, 108, 92, 0, 0, 0,
	0, 0, 123, 125, 0, 0, 129, 146, 160, 0,
	162, 0, 173, 0, 0, 207, 207, 196, 27, 0,
	179, 39, 40, 41, 231, 58, 138, 140, 204, 0,
	186, 0, 86, 89, 96, 0, 98, 0, 100, 101,
	102, 87, 0, 0, 0, 93, 88, 104, 198, 199,
	0, 126, 161, 169, 176, 0, 22, 0, 23, 26,
	189, 190, 193, 55, 0, 206, 188, 0, 0, 0,
	97, 99, 0, 0, 0, 127, 174, 0, 0, 192,
	194, 195, 141, 196, 187, 185, 90, 0, 0, 0,
	0, 191, 200, 0, 112, 0, 0, 0, 20, 0,
	0, 109, 0, 110, 111, 208, 201, 0, 113, 0,
	202,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 81, 76, 3,
	39, 118, 79, 77, 45, 78, 83, 80, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	66, 65, 67, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 82, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 75, 3, 40,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 41, 42, 43,
	44, 46, 47, 48, 49, 50, 51, 52, 53, 54,
	55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
	68, 69, 70, 71, 72, 73, 74, 84, 85, 86,
	87, 88, 89, 90, 91, 92, 93, 94, 95, 96,
	97, 98, 99, 100, 101, 102, 103, 104, 105, 106,
	107, 108, 109, 110, 111, 112, 113, 114, 115, 116,
	117,
}
var yyTok3 = [...]int{
	0,
}

var yyErrorMessages = [...]struct {
	state int
	token int
	msg   string
}{}

//line yaccpar:1

/*	parser for yacc output	*/

var (
	yyDebug        = 0
	yyErrorVerbose = false
)

type yyLexer interface {
	Lex(lval *yySymType) int
	Error(s string)
}

type yyParser interface {
	Parse(yyLexer) int
	Lookahead() int
}

type yyParserImpl struct {
	lval  yySymType
	stack [yyInitialStackSize]yySymType
	char  int
}

func (p *yyParserImpl) Lookahead() int {
	return p.char
}

func yyNewParser() yyParser {
	return &yyParserImpl{}
}

const yyFlag = -1000

func yyTokname(c int) string {
	if c >= 1 && c-1 < len(yyToknames) {
		if yyToknames[c-1] != "" {
			return yyToknames[c-1]
		}
	}
	return __yyfmt__.Sprintf("tok-%v", c)
}

func yyStatname(s int) string {
	if s >= 0 && s < len(yyStatenames) {
		if yyStatenames[s] != "" {
			return yyStatenames[s]
		}
	}
	return __yyfmt__.Sprintf("state-%v", s)
}

func yyErrorMessage(state, lookAhead int) string {
	const TOKSTART = 4

	if !yyErrorVerbose {
		return "syntax error"
	}

	for _, e := range yyErrorMessages {
		if e.state == state && e.token == lookAhead {
			return "syntax error: " + e.msg
		}
	}

	res := "syntax error: unexpected " + yyTokname(lookAhead)

	// To match Bison, suggest at most four expected tokens.
	expected := make([]int, 0, 4)

	// Look for shiftable tokens.
	base := yyPact[state]
	for tok := TOKSTART; tok-1 < len(yyToknames); tok++ {
		if n := base + tok; n >= 0 && n < yyLast && yyChk[yyAct[n]] == tok {
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}
	}

	if yyDef[state] == -2 {
		i := 0
		for yyExca[i] != -1 || yyExca[i+1] != state {
			i += 2
		}

		// Look for tokens that we accept or reduce.
		for i += 2; yyExca[i] >= 0; i += 2 {
			tok := yyExca[i]
			if tok < TOKSTART || yyExca[i+1] == 0 {
				continue
			}
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}

		// If the default action is to accept or reduce, give up.
		if yyExca[i+1] != 0 {
			return res
		}
	}

	for i, tok := range expected {
		if i == 0 {
			res += ", expecting "
		} else {
			res += " or "
		}
		res += yyTokname(tok)
	}
	return res
}

func yylex1(lex yyLexer, lval *yySymType) (char, token int) {
	token = 0
	char = lex.Lex(lval)
	if char <= 0 {
		token = yyTok1[0]
		goto out
	}
	if char < len(yyTok1) {
		token = yyTok1[char]
		goto out
	}
	if char >= yyPrivate {
		if char < yyPrivate+len(yyTok2) {
			token = yyTok2[char-yyPrivate]
			goto out
		}
	}
	for i := 0; i < len(yyTok3); i += 2 {
		token = yyTok3[i+0]
		if token == char {
			token = yyTok3[i+1]
			goto out
		}
	}

out:
	if token == 0 {
		token = yyTok2[1] /* unknown char */
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("lex %s(%d)\n", yyTokname(token), uint(char))
	}
	return char, token
}

func yyParse(yylex yyLexer) int {
	return yyNewParser().Parse(yylex)
}

func (yyrcvr *yyParserImpl) Parse(yylex yyLexer) int {
	var yyn int
	var yyVAL yySymType
	var yyDollar []yySymType
	_ = yyDollar // silence set and not used
	yyS := yyrcvr.stack[:]

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yystate := 0
	yyrcvr.char = -1
	yytoken := -1 // yyrcvr.char translated into internal numbering
	defer func() {
		// Make sure we report no lookahead when not parsing.
		yystate = -1
		yyrcvr.char = -1
		yytoken = -1
	}()
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	if yyDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", yyTokname(yytoken), yyStatname(yystate))
	}

	yyp++
	if yyp >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyS[yyp] = yyVAL
	yyS[yyp].yys = yystate

yynewstate:
	yyn = yyPact[yystate]
	if yyn <= yyFlag {
		goto yydefault /* simple state */
	}
	if yyrcvr.char < 0 {
		yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
	}
	yyn += yytoken
	if yyn < 0 || yyn >= yyLast {
		goto yydefault
	}
	yyn = yyAct[yyn]
	if yyChk[yyn] == yytoken { /* valid shift */
		yyrcvr.char = -1
		yytoken = -1
		yyVAL = yyrcvr.lval
		yystate = yyn
		if Errflag > 0 {
			Errflag--
		}
		goto yystack
	}

yydefault:
	/* default state action */
	yyn = yyDef[yystate]
	if yyn == -2 {
		if yyrcvr.char < 0 {
			yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
		}

		/* look through exception table */
		xi := 0
		for {
			if yyExca[xi+0] == -1 && yyExca[xi+1] == yystate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			yyn = yyExca[xi+0]
			if yyn < 0 || yyn == yytoken {
				break
			}
		}
		yyn = yyExca[xi+1]
		if yyn < 0 {
			goto ret0
		}
	}
	if yyn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			yylex.Error(yyErrorMessage(yystate, yytoken))
			Nerrs++
			if yyDebug >= 1 {
				__yyfmt__.Printf("%s", yyStatname(yystate))
				__yyfmt__.Printf(" saw %s\n", yyTokname(yytoken))
			}
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for yyp >= 0 {
				yyn = yyPact[yyS[yyp].yys] + yyErrCode
				if yyn >= 0 && yyn < yyLast {
					yystate = yyAct[yyn] /* simulate a shift of "error" */
					if yyChk[yystate] == yyErrCode {
						goto yystack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if yyDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", yyTokname(yytoken))
			}
			if yytoken == yyEofCode {
				goto ret1
			}
			yyrcvr.char = -1
			yytoken = -1
			goto yynewstate /* try again in the same state */
		}
	}

	/* reduction by production yyn */
	if yyDebug >= 2 {
		__yyfmt__.Printf("reduce %v in:\n\t%v\n", yyn, yyStatname(yystate))
	}

	yynt := yyn
	yypt := yyp
	_ = yypt // guard against "declared and not used"

	yyp -= yyR2[yyn]
	// yyp is now the index of $0. Perform the default action. Iff the
	// reduced production is ε, $1 is possibly out of range.
	if yyp+1 >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	yyn = yyR1[yyn]
	yyg := yyPgo[yyn]
	yyj := yyg + yyS[yyp].yys + 1

	if yyj >= yyLast {
		yystate = yyAct[yyg]
	} else {
		yystate = yyAct[yyj]
		if yyChk[yystate] != -yyn {
			yystate = yyAct[yyg]
		}
	}
	// dummy call; replaced with literal code
	switch yynt {

	case 1:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:200
		{
			SetParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:206
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 19:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:228
		{
			yyVAL.selStmt = &SimpleSelect{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, SelectExprs: yyDollar[4].selectExprs, Limit: yyDollar[5].limit}
		}
	case 20:
		yyDollar = yyS[yypt-12 : yypt+1]
		//line sql.y:232
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, SelectExprs: yyDollar[4].selectExprs, From: yyDollar[6].tableExprs, Where: NewWhere(AST_WHERE, yyDollar[7].boolExpr), GroupBy: GroupBy(yyDollar[8].valExprs), Having: NewWhere(AST_HAVING, yyDollar[9].boolExpr), OrderBy: yyDollar[10].orderBy, Limit: yyDollar[11].limit, Lock: yyDollar[12].str}
		}
	case 21:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:236
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 22:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:243
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[5].tableName, Columns: yyDollar[6].columns, Rows: yyDollar[7].insRows, OnDup: OnDup(yyDollar[8].updateExprs)}
		}
	case 23:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:247
		{
			cols := make(Columns, 0, len(yyDollar[7].updateExprs))
			vals := make(ValTuple, 0, len(yyDollar[7].updateExprs))
			for _, col := range yyDollar[7].updateExprs {
				cols = append(cols, &NonStarExpr{Expr: col.Name})
				vals = append(vals, col.Expr)
			}
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[5].tableName, Columns: cols, Rows: Values{vals}, OnDup: OnDup(yyDollar[8].updateExprs)}
		}
	case 24:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:259
		{
			yyVAL.statement = &Replace{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Columns: yyDollar[5].columns, Rows: yyDollar[6].insRows}
		}
	case 25:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:263
		{
			cols := make(Columns, 0, len(yyDollar[6].updateExprs))
			vals := make(ValTuple, 0, len(yyDollar[6].updateExprs))
			for _, col := range yyDollar[6].updateExprs {
				cols = append(cols, &NonStarExpr{Expr: col.Name})
				vals = append(vals, col.Expr)
			}
			yyVAL.statement = &Replace{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Columns: cols, Rows: Values{vals}}
		}
	case 26:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:276
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(AST_WHERE, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 27:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:282
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(AST_WHERE, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 28:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:288
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 29:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:292
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: UpdateExprs{&UpdateExpr{Name: &ColName{Name: []byte("names")}, Expr: StrVal("default")}}}
		}
	case 30:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:296
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: UpdateExprs{&UpdateExpr{Name: &ColName{Name: []byte("names")}, Expr: yyDollar[4].valExpr}}}
		}
	case 31:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:300
		{
			yyVAL.statement = &Set{
				Comments: Comments(yyDollar[2].bytes2),
				Exprs: UpdateExprs{
					&UpdateExpr{
						Name: &ColName{Name: []byte("names")}, Expr: yyDollar[4].valExpr,
					},
					&UpdateExpr{
						Name: &ColName{Name: []byte("collate")}, Expr: yyDollar[6].valExpr,
					},
				},
			}
		}
	case 32:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:314
		{
			yyVAL.statement = &Set{
				Exprs: UpdateExprs{
					&UpdateExpr{
						Name: &ColName{Name: []byte("transaction")},
					},
				},
			}
		}
	case 33:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:324
		{
			yyVAL.statement = &Set{
				Exprs: UpdateExprs{
					&UpdateExpr{
						Name: &ColName{Name: []byte("transaction")},
					},
				},
			}
		}
	case 43:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:350
		{
			yyVAL.bytes2 = nil
		}
	case 44:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:354
		{
			yyVAL.bytes2 = [][]byte{yyDollar[1].bytes}
		}
	case 45:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:358
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[3].bytes)
		}
	case 46:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:364
		{
			yyVAL.statement = &Begin{}
		}
	case 47:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:368
		{
			yyVAL.statement = &Begin{}
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:375
		{
			yyVAL.statement = &Commit{}
		}
	case 49:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:381
		{
			yyVAL.statement = &Rollback{}
		}
	case 50:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:387
		{
			yyVAL.statement = &Admin{Command: yyDollar[2].bytes, Args: yyDollar[4].bytes2}
		}
	case 51:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:393
		{
			yyVAL.statement = &Describe{TableName: yyDollar[2].bytes}
		}
	case 52:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:399
		{
			yyVAL.statement = &UseDB{DB: string(yyDollar[2].bytes)}
		}
	case 53:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:405
		{
			yyVAL.statement = &Truncate{Comments: Comments(yyDollar[2].bytes2), TableOpt: yyDollar[3].str, Table: yyDollar[4].tableName}
		}
	case 54:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:411
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyDollar[4].bytes}
		}
	case 55:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:415
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyDollar[7].bytes, NewName: yyDollar[7].bytes}
		}
	case 56:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:420
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyDollar[3].bytes}
		}
	case 57:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:426
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Ignore: yyDollar[2].str, Table: yyDollar[4].bytes, NewName: yyDollar[4].bytes}
		}
	case 58:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:430
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: AST_RENAME, Ignore: yyDollar[2].str, Table: yyDollar[4].bytes, NewName: yyDollar[7].bytes}
		}
	case 59:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:435
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyDollar[3].bytes, NewName: yyDollar[3].bytes}
		}
	case 60:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:441
		{
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyDollar[3].bytes, NewName: yyDollar[5].bytes}
		}
	case 61:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:447
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyDollar[4].bytes}
		}
	case 62:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:451
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyDollar[5].bytes, NewName: yyDollar[5].bytes}
		}
	case 63:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:456
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyDollar[4].bytes}
		}
	case 64:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:461
		{
			SetAllowComments(yylex, true)
		}
	case 65:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:465
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			SetAllowComments(yylex, false)
		}
	case 66:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:471
		{
			yyVAL.bytes2 = nil
		}
	case 67:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:475
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 68:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:481
		{
			yyVAL.str = AST_UNION
		}
	case 69:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:485
		{
			yyVAL.str = AST_UNION_ALL
		}
	case 70:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:489
		{
			yyVAL.str = AST_SET_MINUS
		}
	case 71:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:493
		{
			yyVAL.str = AST_EXCEPT
		}
	case 72:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:497
		{
			yyVAL.str = AST_INTERSECT
		}
	case 73:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:502
		{
			yyVAL.str = ""
		}
	case 74:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:506
		{
			yyVAL.str = AST_DISTINCT
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:512
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 76:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:516
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 77:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:522
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 78:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:526
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].bytes}
		}
	case 79:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:530
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyDollar[1].bytes}
		}
	case 80:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:536
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 81:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:540
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 82:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:545
		{
			yyVAL.bytes = nil
		}
	case 83:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:549
		{
			yyVAL.bytes = yyDollar[1].bytes
		}
	case 84:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:553
		{
			yyVAL.bytes = yyDollar[2].bytes
		}
	case 85:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:559
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 86:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:563
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 87:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:569
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].smTableExpr, As: yyDollar[2].bytes, Hints: yyDollar[3].indexHints}
		}
	case 88:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:573
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyDollar[2].tableExpr}
		}
	case 89:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:577
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 90:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:581
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 91:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:586
		{
			yyVAL.bytes = nil
		}
	case 92:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:590
		{
			yyVAL.bytes = yyDollar[1].bytes
		}
	case 93:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:594
		{
			yyVAL.bytes = yyDollar[2].bytes
		}
	case 94:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:600
		{
			yyVAL.str = AST_JOIN
		}
	case 95:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:604
		{
			yyVAL.str = AST_STRAIGHT_JOIN
		}
	case 96:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:608
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 97:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:612
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 98:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:616
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 99:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:620
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 100:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:624
		{
			yyVAL.str = AST_JOIN
		}
	case 101:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:628
		{
			yyVAL.str = AST_CROSS_JOIN
		}
	case 102:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:632
		{
			yyVAL.str = AST_NATURAL_JOIN
		}
	case 103:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:638
		{
			yyVAL.smTableExpr = &TableName{Name: yyDollar[1].bytes}
		}
	case 104:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:642
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyDollar[1].bytes, Name: yyDollar[3].bytes}
		}
	case 105:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:646
		{
			yyVAL.smTableExpr = yyDollar[1].subquery
		}
	case 106:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:652
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].bytes}
		}
	case 107:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:656
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].bytes, Name: yyDollar[3].bytes}
		}
	case 108:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:661
		{
			yyVAL.indexHints = nil
		}
	case 109:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:665
		{
			yyVAL.indexHints = &IndexHints{Type: AST_USE, Indexes: yyDollar[4].bytes2}
		}
	case 110:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:669
		{
			yyVAL.indexHints = &IndexHints{Type: AST_IGNORE, Indexes: yyDollar[4].bytes2}
		}
	case 111:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:673
		{
			yyVAL.indexHints = &IndexHints{Type: AST_FORCE, Indexes: yyDollar[4].bytes2}
		}
	case 112:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:679
		{
			yyVAL.bytes2 = [][]byte{yyDollar[1].bytes}
		}
	case 113:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:683
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[3].bytes)
		}
	case 114:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:688
		{
			yyVAL.boolExpr = nil
		}
	case 115:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:692
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 117:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:699
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 118:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:703
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 119:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:707
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 120:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:711
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 121:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:717
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 122:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:721
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_IN, Right: yyDollar[3].tuple}
		}
	case 123:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:725
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_NOT_IN, Right: yyDollar[4].tuple}
		}
	case 124:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:729
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_LIKE, Right: yyDollar[3].valExpr}
		}
	case 125:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:733
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_NOT_LIKE, Right: yyDollar[4].valExpr}
		}
	case 126:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:737
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: AST_BETWEEN, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 127:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:741
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: AST_NOT_BETWEEN, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 128:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:745
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NULL, Expr: yyDollar[1].valExpr}
		}
	case 129:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:749
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NOT_NULL, Expr: yyDollar[1].valExpr}
		}
	case 130:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:753
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:759
		{
			yyVAL.str = AST_EQ
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:763
		{
			yyVAL.str = AST_LT
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:767
		{
			yyVAL.str = AST_GT
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:771
		{
			yyVAL.str = AST_LE
		}
	case 135:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:775
		{
			yyVAL.str = AST_GE
		}
	case 136:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:779
		{
			yyVAL.str = AST_NE
		}
	case 137:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:783
		{
			yyVAL.str = AST_NSE
		}
	case 138:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:789
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 139:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:793
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 140:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:799
		{
			yyVAL.values = Values{yyDollar[1].tuple}
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:803
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].tuple)
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:809
		{
			yyVAL.tuple = ValTuple(yyDollar[2].valExprs)
		}
	case 143:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:813
		{
			yyVAL.tuple = yyDollar[1].subquery
		}
	case 144:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:819
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 145:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:825
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 146:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:829
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 147:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:835
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 148:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:839
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 149:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:843
		{
			yyVAL.valExpr = yyDollar[1].tuple
		}
	case 150:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:847
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITAND, Right: yyDollar[3].valExpr}
		}
	case 151:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:851
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITOR, Right: yyDollar[3].valExpr}
		}
	case 152:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:855
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITXOR, Right: yyDollar[3].valExpr}
		}
	case 153:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:859
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_PLUS, Right: yyDollar[3].valExpr}
		}
	case 154:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:863
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MINUS, Right: yyDollar[3].valExpr}
		}
	case 155:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:867
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MULT, Right: yyDollar[3].valExpr}
		}
	case 156:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:871
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_DIV, Right: yyDollar[3].valExpr}
		}
	case 157:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:875
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MOD, Right: yyDollar[3].valExpr}
		}
	case 158:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:879
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				switch yyDollar[1].byt {
				case '-':
					yyVAL.valExpr = append(NumVal("-"), num...)
				case '+':
					yyVAL.valExpr = num
				default:
					yyVAL.valExpr = &UnaryExpr{Operator: yyDollar[1].byt, Expr: yyDollar[2].valExpr}
				}
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: yyDollar[1].byt, Expr: yyDollar[2].valExpr}
			}
		}
	case 159:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:894
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].bytes}
		}
	case 160:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:898
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].bytes, Exprs: yyDollar[3].selectExprs}
		}
	case 161:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:902
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].bytes, Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 162:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:906
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].bytes, Exprs: yyDollar[3].selectExprs}
		}
	case 163:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:910
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 164:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:916
		{
			yyVAL.bytes = IF_BYTES
		}
	case 165:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:920
		{
			yyVAL.bytes = VALUES_BYTES
		}
	case 166:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:926
		{
			yyVAL.byt = AST_UPLUS
		}
	case 167:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:930
		{
			yyVAL.byt = AST_UMINUS
		}
	case 168:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:934
		{
			yyVAL.byt = AST_TILDA
		}
	case 169:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:940
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 170:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:945
		{
			yyVAL.valExpr = nil
		}
	case 171:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:949
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 172:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:955
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 173:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:959
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 174:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:965
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 175:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:970
		{
			yyVAL.valExpr = nil
		}
	case 176:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:974
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 177:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:980
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].bytes}
		}
	case 178:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:984
		{
			yyVAL.colName = &ColName{Qualifier: yyDollar[1].bytes, Name: yyDollar[3].bytes}
		}
	case 179:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:988
		{
			yyVAL.colName = &ColName{Qualifier: yyDollar[3].bytes, Name: yyDollar[5].bytes}
		}
	case 180:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:994
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 181:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:998
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 182:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1002
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 183:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1006
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 184:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1011
		{
			yyVAL.valExprs = nil
		}
	case 185:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1015
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 186:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1020
		{
			yyVAL.boolExpr = nil
		}
	case 187:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1024
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 188:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1029
		{
			yyVAL.orderBy = nil
		}
	case 189:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1033
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 190:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1039
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 191:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1043
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 192:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1049
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 193:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1054
		{
			yyVAL.str = AST_ASC
		}
	case 194:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1058
		{
			yyVAL.str = AST_ASC
		}
	case 195:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1062
		{
			yyVAL.str = AST_DESC
		}
	case 196:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1067
		{
			yyVAL.limit = nil
		}
	case 197:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1071
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 198:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1075
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 199:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1079
		{
			yyVAL.limit = &Limit{Offset: yyDollar[4].valExpr, Rowcount: yyDollar[2].valExpr}
		}
	case 200:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1084
		{
			yyVAL.str = ""
		}
	case 201:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1088
		{
			yyVAL.str = AST_FOR_UPDATE
		}
	case 202:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1092
		{
			if !bytes.Equal(yyDollar[3].bytes, SHARE) {
				yylex.Error("expecting share")
				return 1
			}
			if !bytes.Equal(yyDollar[4].bytes, MODE) {
				yylex.Error("expecting mode")
				return 1
			}
			yyVAL.str = AST_SHARE_MODE
		}
	case 203:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1105
		{
			yyVAL.columns = nil
		}
	case 204:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1109
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 205:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1115
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyDollar[1].colName}}
		}
	case 206:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1119
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyDollar[3].colName})
		}
	case 207:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1124
		{
			yyVAL.updateExprs = nil
		}
	case 208:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1128
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 209:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1134
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 210:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1138
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 211:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1144
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colName, Expr: yyDollar[3].valExpr}
		}
	case 212:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1148
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colName, Expr: StrVal("ON")}
		}
	case 213:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1153
		{
			yyVAL.empty = struct{}{}
		}
	case 214:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1155
		{
			yyVAL.empty = struct{}{}
		}
	case 215:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1158
		{
			yyVAL.empty = struct{}{}
		}
	case 216:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1160
		{
			yyVAL.empty = struct{}{}
		}
	case 217:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1163
		{
			yyVAL.str = ""
		}
	case 218:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1165
		{
			yyVAL.str = AST_IGNORE
		}
	case 219:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1169
		{
			yyVAL.empty = struct{}{}
		}
	case 220:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1171
		{
			yyVAL.empty = struct{}{}
		}
	case 221:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1173
		{
			yyVAL.empty = struct{}{}
		}
	case 222:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1175
		{
			yyVAL.empty = struct{}{}
		}
	case 223:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1177
		{
			yyVAL.empty = struct{}{}
		}
	case 224:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1180
		{
			yyVAL.empty = struct{}{}
		}
	case 225:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1182
		{
			yyVAL.empty = struct{}{}
		}
	case 226:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1185
		{
			yyVAL.empty = struct{}{}
		}
	case 227:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1187
		{
			yyVAL.empty = struct{}{}
		}
	case 228:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1190
		{
			yyVAL.empty = struct{}{}
		}
	case 229:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1192
		{
			yyVAL.empty = struct{}{}
		}
	case 230:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1196
		{
			yyVAL.bytes = bytes.ToLower(yyDollar[1].bytes)
		}
	case 231:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1201
		{
			ForceEOF(yylex)
		}
	case 232:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1206
		{
			yyVAL.str = ""
		}
	case 233:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1210
		{
			yyVAL.str = AST_TABLE
		}
	}
	goto yystack /* stack new state and value */
}
