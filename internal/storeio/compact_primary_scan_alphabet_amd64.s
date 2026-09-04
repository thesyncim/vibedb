//go:build amd64

#include "textflag.h"

// func compactScanAlphabet5BMI2(dst *byte, dstLen int, data *byte,
//     dataLen, bit int, base byte) int
TEXT ·compactScanAlphabet5BMI2(SB), NOSPLIT, $0-56
	MOVQ dst+0(FP), DI
	MOVQ dstLen+8(FP), SI
	MOVQ data+16(FP), DX
	MOVQ dataLen+24(FP), R8
	MOVQ bit+32(FP), R9
	MOVBQZX base+40(FP), R10
	MOVQ $0x0101010101010101, R12
	IMULQ R12, R10
	MOVQ $0x1f1f1f1f1f1f1f1f, R11
	XORQ AX, AX

loop:
	LEAQ 8(AX), BX
	CMPQ BX, SI
	JG done
	MOVQ R9, CX
	SHRQ $3, CX
	LEAQ 8(CX), BX
	CMPQ BX, R8
	JG done
	MOVQ (DX)(CX*1), BX
	MOVQ R9, CX
	ANDQ $7, CX
	SHRQ CX, BX
	PDEPQ R11, BX, BX
	ADDQ R10, BX
	MOVQ BX, (DI)(AX*1)
	ADDQ $8, AX
	ADDQ $40, R9
	JMP loop

done:
	MOVQ AX, ret+48(FP)
	RET
