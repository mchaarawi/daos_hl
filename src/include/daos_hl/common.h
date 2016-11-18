/**
 * (C) Copyright 2016 Intel Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * GOVERNMENT LICENSE RIGHTS-OPEN SOURCE SOFTWARE
 * The Government's rights to use, modify, reproduce, release, perform, display,
 * or disclose this software are subject to the terms of the Apache License as
 * provided in Contract No. B609815.
 * Any reproduction of computer software, computer software documentation, or
 * portions thereof marked with this legend must also reproduce the markings. */

#ifndef __DAOS_HL_COMMON_H__
#define __DAOS_HL_COMMON_H__

#include <assert.h>
#include <stdio.h>

#define D_ERROR(fmt, ...)						\
do {									\
	fprintf(stderr, "%s:%d:%d:%s() " fmt, __FILE__, getpid(),	\
		__LINE__, __func__, ## __VA_ARGS__);			\
	fflush(stderr);							\
} while (0)

#define D_ASSERT(e)	assert(e)

#define D_ASSERTF(cond, fmt, ...)					\
do {									\
	if (!(cond))							\
		D_ERROR(fmt, ## __VA_ARGS__);				\
	assert(cond);							\
} while (0)

#endif /* __DAOS_HL_COMMON_H__ */
