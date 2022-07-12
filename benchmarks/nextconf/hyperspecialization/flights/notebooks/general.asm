00000000000013b0 <process_cells>:
    13b0:	41 56                	push   %r14
    13b2:	53                   	push   %rbx
    13b3:	50                   	push   %rax
    13b4:	4c 8b 76 10          	mov    0x10(%rsi),%r14
    13b8:	48 8b 5e 18          	mov    0x18(%rsi),%rbx
    13bc:	48 8b be c8 01 00 00 	mov    0x1c8(%rsi),%rdi
    13c3:	80 3f 00             	cmpb   $0x0,(%rdi)
    13c6:	74 09                	je     13d1 <process_cells+0x21>
    13c8:	31 f6                	xor    %esi,%esi
    13ca:	e8 a1 fc ff ff       	callq  1070 <strtod@plt>
    13cf:	eb 04                	jmp    13d5 <process_cells+0x25>
    13d1:	c5 f9 57 c0          	vxorpd %xmm0,%xmm0,%xmm0
    13d5:	c5 fb 11 04 24       	vmovsd %xmm0,(%rsp)
    13da:	48 89 df             	mov    %rbx,%rdi
    13dd:	31 f6                	xor    %esi,%esi
    13df:	ba 0a 00 00 00       	mov    $0xa,%edx
    13e4:	e8 d7 fc ff ff       	callq  10c0 <strtol@plt>
    13e9:	48 89 c3             	mov    %rax,%rbx
    13ec:	4c 89 f7             	mov    %r14,%rdi
    13ef:	31 f6                	xor    %esi,%esi
    13f1:	ba 0a 00 00 00       	mov    $0xa,%edx
    13f6:	e8 c5 fc ff ff       	callq  10c0 <strtol@plt>
    13fb:	c1 e3 04             	shl    $0x4,%ebx
    13fe:	01 c3                	add    %eax,%ebx
    1400:	48 8b 05 89 2c 00 00 	mov    0x2c89(%rip),%rax        # 4090 <_ZL7agg_map>
    1407:	48 63 cb             	movslq %ebx,%rcx
    140a:	48 c1 e1 05          	shl    $0x5,%rcx
    140e:	80 3c 08 00          	cmpb   $0x0,(%rax,%rcx,1)
    1412:	74 16                	je     142a <process_cells+0x7a>
    1414:	48 8b 54 08 08       	mov    0x8(%rax,%rcx,1),%rdx
    1419:	c5 fb 10 4c 08 10    	vmovsd 0x10(%rax,%rcx,1),%xmm1
    141f:	c5 fb 10 44 08 18    	vmovsd 0x18(%rax,%rcx,1),%xmm0
    1425:	48 ff c2             	inc    %rdx
    1428:	eb 27                	jmp    1451 <process_cells+0xa1>
    142a:	48 8d 14 08          	lea    (%rax,%rcx,1),%rdx
    142e:	c6 02 01             	movb   $0x1,(%rdx)
    1431:	c5 f9 57 c0          	vxorpd %xmm0,%xmm0,%xmm0
    1435:	c5 f9 11 44 08 08    	vmovupd %xmm0,0x8(%rax,%rcx,1)
    143b:	48 c7 44 08 18 00 00 	movq   $0x0,0x18(%rax,%rcx,1)
    1442:	00 00 
    1444:	ba 01 00 00 00       	mov    $0x1,%edx
    1449:	c5 f9 57 c0          	vxorpd %xmm0,%xmm0,%xmm0
    144d:	c5 f1 57 c9          	vxorpd %xmm1,%xmm1,%xmm1
    1451:	c4 e1 eb 2a d2       	vcvtsi2sd %rdx,%xmm2,%xmm2
    1456:	c5 fb 10 24 24       	vmovsd (%rsp),%xmm4
    145b:	c5 db 5c d9          	vsubsd %xmm1,%xmm4,%xmm3
    145f:	c5 e3 5e d2          	vdivsd %xmm2,%xmm3,%xmm2
    1463:	c5 f3 58 ca          	vaddsd %xmm2,%xmm1,%xmm1
    1467:	c5 db 5c d1          	vsubsd %xmm1,%xmm4,%xmm2
    146b:	c5 e3 59 d2          	vmulsd %xmm2,%xmm3,%xmm2
    146f:	c5 fb 58 c2          	vaddsd %xmm2,%xmm0,%xmm0
    1473:	48 89 54 08 08       	mov    %rdx,0x8(%rax,%rcx,1)
    1478:	c5 fb 11 4c 08 10    	vmovsd %xmm1,0x10(%rax,%rcx,1)
    147e:	c5 fb 11 44 08 18    	vmovsd %xmm0,0x18(%rax,%rcx,1)
    1484:	31 c0                	xor    %eax,%eax
    1486:	48 83 c4 08          	add    $0x8,%rsp
    148a:	5b                   	pop    %rbx
    148b:	41 5e                	pop    %r14
    148d:	c3                   	retq   
    148e:	66 90                	xchg   %ax,%ax
