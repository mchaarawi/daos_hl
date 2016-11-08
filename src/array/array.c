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
 * portions thereof marked with this legend must also reproduce the markings.
 */
/**
 * This file is part of daos_hl
 *
 * src/array/array.c
 */

#include <daos_api.h>

/** MSC - Those need to be configurable later through hints */
/** Array cell size - curently a byte array i.e. 1 byte */
#define DAOS_HL_CELL_SIZE 1
/** Bytes to store in a dkey before moving to the next one in the group */
#define DAOS_HL_DKEY_BLOCK_SIZE		1048576
/** Num blocks to store in each dkey before creating the next group */
#define DAOS_HL_DKEY_NUM_BLOCKS		3
/** Number of dkeys in a group */
#define DAOS_HL_DKEY_NUM		4
#define DAOS_HL_DKEY_GRP_CHUNK		(DAOS_HL_DKEY_BLOCK_SIZE * \
					 DAOS_HL_DKEY_NUM)
#define DAOS_HL_DKEY_GRP_SIZE		(DAOS_HL_DKEY_BLOCK_SIZE * \
					 DAOS_HL_DKEY_NUM_BLOCKS * \
					 DAOS_HL_DKEY_NUM)

typedef enum {
	DAOS_HL_OP_WRITE,
	DAOS_HL_OP_READ,
} daos_hl_op_type_t;

static int
daos_hl_extent_same(daos_hl_array_ranges_t *ranges, daos_sg_list_t *sgl);

static int
compute_dkey(daos_off_t array_i, daos_size_t *num_records,
	     daos_off_t *record_i, daos_key_t *obj_dkey);

static int
create_sgl(daos_sg_list_t *user_sgl, daos_size_t num_records,
	   daos_off_t *sgl_off, daos_size_t *sgl_i, daos_sg_list_t *sgl);

static int
daos_hl_access_obj(daos_handle_t oh, daos_epoch_t epoch,
		   daos_hl_array_ranges_t *ranges, daos_sg_list_t *sgl,
		   daos_csum_buf_t *csums, daos_event_t *ev,
		   daos_hl_op_type_t op_type);

static int
daos_hl_extent_same(daos_hl_array_ranges_t *ranges, daos_sg_list_t *sgl)
{
	daos_size_t ranges_len;
	daos_size_t sgl_len;
	daos_size_t u;

	ranges_len = 0;
	for (u=0 ; u<ranges->ranges_nr ; u++)
		ranges_len += ranges->range[u].len;

	sgl_len = 0;
	for (u=0 ; u<sgl->sg_nr.num; u++)
		sgl_len += sgl->sg_iovs[u].iov_len;

	return ((ranges_len == sgl_len) ? 1 : 0);
}

static int
compute_dkey(daos_off_t array_i, daos_size_t *num_records, daos_off_t *record_i,
	     daos_key_t *obj_dkey)
{
	daos_off_t 	byte_a; 	/* Byte address of I/O */
	daos_size_t 	dkey_grp; 	/* Which grp of dkeys to look into */
	daos_off_t 	dkey_grp_a; 	/* Byte address of dkey_grp */
	daos_off_t 	rel_byte_a; 	/* offset relative to grp */
	daos_size_t 	dkey_num; 	/* The dkey number for access */
	daos_size_t 	grp_iter; 	/* round robin iteration number */
	char 		*dkey;

	byte_a = array_i * DAOS_HL_CELL_SIZE;

	/* Compute dkey group number and address */
	dkey_grp = byte_a / DAOS_HL_DKEY_GRP_SIZE;
	dkey_grp_a = dkey_grp * DAOS_HL_DKEY_GRP_SIZE;

	/* Compute dkey number within dkey group */
	rel_byte_a = byte_a - dkey_grp_a;
	dkey_num = (size_t)(rel_byte_a / DAOS_HL_DKEY_BLOCK_SIZE) %
		DAOS_HL_DKEY_NUM;

	/* Compute relative offset/index in dkey */
	grp_iter = rel_byte_a / DAOS_HL_DKEY_GRP_CHUNK;
	dkey_byte_a = (grp_iter * DAOS_HL_DKEY_GRP_CHUNK) +
		(dkey_num * DAOS_HL_DKEY_BLOCK_SIZE);
	record_i = (DAOS_HL_DKEY_BLOCK_SIZE * grp_iter) +
		(rel_byte_a - dkey_byte_a);

	/* Number of records to access in current dkey */
	num_records = ((grp_iter + 1) * DAOS_HL_DKEY_BLOCK_SIZE) - record_i;
	asprintf(dkey, "%zu_%zu", dkey_grp, dkey_num);
	if (NULL == dkey) {
		D_ERROR("Failed memory allocation\n");
		return -1;
	}

	daos_iov_set(obj_dkey, (void *)dkey, strlen(dkey));

	if (dkey) {
		free(dkey);
		dkey = NULL;
	}
	return 0;
}

static int
create_sgl(daos_sg_list_t *user_sgl, daos_size_t num_records,
	   daos_off_t *sgl_off, daos_size_t *sgl_i, daos_sg_list_t *sgl)
{
	daos_size_t 	k;
	daos_size_t 	rem_records;
	daos_size_t	cur_i;
	daos_off_t	cur_off;

	cur_i = *sgl_i;
	cur_off = *sgl_off;
	sgl->sg_nr.num = k = 0;
	rem_records = num_records;

	/* 
	 * Keep iterating through the user sgl till we populate our sgl to
	 * satisfy the number of records to read/write from the KV object
	 */
	do {
		assert(user_sgl->sg_nr.num > cur_i);

		sgl->sg_nr.num ++;
		sgl->sg_iovs = (daos_iov_t *)realloc(sgl->sg_iovs,
						     sizeof(daos_iov_t) *
						     sgl->sg_nr.num);

		sgl->sg_iovs[k].iov_buf = user_sgl->sg_iovs[cur_i].iov_buf +
			cur_off;
		if (rem_records >= user_sgl->sg_iovs[cur_i].iov_len) {
			sgl->iov_len = user_sgl->sg_iovs[cur_i].iov_len;
			cur_i ++;
			cur_off = 0;
		}
		else {
			sgl->iov_len = rem_records;
			cur_off += rem_records;
		}
		sgl->iov_buf_len = sgl->iov_len;
		rem_records -= sgl->iov_len;
	} while (rem_records);

	*sgl_i = cur_i;
	*sgl_off = cur_off;
	return 0;
}

static int
daos_hl_access_obj(daos_handle_t oh, daos_epoch_t epoch,
		   daos_hl_array_ranges_t *ranges, daos_sg_list_t *sgl,
		   daos_csum_buf_t *csums, daos_event_t *ev,
		   daos_hl_op_type_t op_type)
{
	int rc;

	if (NULL == ranges) {
		D_ERROR("NULL ranges passed\n");
		return -1;
	}
	if (NULL == sgl) {
		D_ERROR("NULL scatter-gather list passed\n");
		return -1;
	}

	rc = daos_hl_extent_same(ranges, sgl);
	if (1 != rc) {
		D_ERROR("Unequal extents of memory and array descriptors\n");
		return -1;
	}

	for (u=0 ; u<ranges->ranges_nr ; u++) {
		daos_size_t records;	/* Number of records to read/write in
					 * current range */
		daos_off_t array_i; 	/* object array index of current
					 * range */
		daos_off_t cur_off; 	/* offset into user buffer sgl to track
					 * current position */
		daos_size_t cur_i; 	/* index into user sgl iovec to track
					 * current position */

		if (0 == ranges->range[u].len)
			continue;

		records = ranges->ranges[u].len;
		array_i = ranges->ranges[u].index;
		cur_off = 0;
		cur_i = 0;

		while (records) {
			daos_key_t 	dkey;
			daos_vec_iod_t 	iod;
			daos_recx_t 	rex;
			daos_sg_list_t 	sgl;

			/* 
			 * For current range, compute dkey of the starting
			 * offset, the index in that dkey, and how many bytes
			 * from the range to read/write
			 */
			rc = compute_dkey(array_i, &num_records, &record_i,
					  &dkey);
			if (rc != 0) {
				D_ERROR("Failed to compute dkey\n");
				return rc;
			}

			/* set descriptor for KV object */
			daos_iov_set(&iod.vd_name, (void *)akey, strlen(akey));
			iod.vd_recxs = &rex;
			iod.vd_recxs[0].rx_rsize = 1;
			iod.vd_recxs[0].rx_idx = record_i;
			iod.vd_recxs[0].rx_nr = num_records;

			/* set sgl for current dkey */
			rc = create_sgl(user_sgl, num_records, &cur_off, &cur_i,
					&sgl);
			if (rc != 0) {
				D_ERROR("Failed to create sgl\n");
				return rc;
			}

			/* issue KV IO to DAOS */

			/* update current range */
			records -= num_records;
			array_i += num_records;

			if(sgl.sg_iovs) {
				free(sgl.sg_iovs);
				sgl.sg_iovs = NULL;
			}
		} /* end while */
	} /* end for */

	return 0;
}

int
daos_hl_array_read(daos_handle_t oh, daos_epoch_t epoch,
		   daos_hl_array_ranges_t *ranges, daos_sg_list_t *sgl,
		   daos_csum_buf_t *csums, daos_event_t *ev)
{
	int rc;

	rc = daos_hl_access_obj(oh, epoch, ranges, sgl, csums, ev,
				DAOS_HL_OP_READ);
	if (0 != rc) {
		D_ERROR("Unequal extents of memory and array descriptors\n");
		return -1;
	}
	
	return rc;
}

int
daos_hl_array_write(daos_handle_t oh, daos_epoch_t epoch,
		    daos_hl_array_ranges_t *ranges, daos_sg_list_t *sgl,
		    daos_csum_buf_t *csums, daos_event_t *ev)
{
	int rc;

	rc = daos_hl_access_obj(oh, epoch, ranges, sgl, csums, ev,
				DAOS_HL_OP_WRITE);
	if (0 != rc) {
		D_ERROR("Unequal extents of memory and array descriptors\n");
		return -1;
	}
	
	return rc;
}
