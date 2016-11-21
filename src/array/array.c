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

#include <daos_hl.h>
#include <daos_hl/common.h>

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
	     daos_off_t *record_i, char **obj_dkey);

static int
create_sgl(daos_sg_list_t *user_sgl, daos_size_t num_records,
	   daos_off_t *sgl_off, daos_size_t *sgl_i, daos_sg_list_t *sgl);

static int
daos_hl_access_obj(daos_handle_t oh, daos_epoch_t epoch,
		   daos_hl_array_ranges_t *ranges, daos_sg_list_t *sgl,
		   daos_csum_buf_t *csums, daos_event_t *ev,
		   daos_hl_op_type_t op_type);

#if 0
static int
daos_hl_parse_env_vars(void)
{
	char *cell_size_val = NULL;
	char *dkey_block_len_val = NULL;
	char *dkey_num_blocks_val = NULL;
	char *num_dkeys_val = NULL;

	cell_size_val = getenv ("DAOS_HL_ARRAY_CELL_SIZE");
	if (cell_size_val) {
		sscanf(cell_size_val, "%zu", &cell_size_g);

		printf("DAOS_HL_ARRAY_CELL_SIZE = %s %zu\n",
		       cell_size_val, cell_size_g);

		if (cell_size_g != 1) {
			D_ERROR("Only a 1 byte cell size is supported.\n");
			return -1;
		}
	}
	else {
		cell_size_g = DAOS_HL_CELL_SIZE;
	}

	dkey_block_len_val = getenv ("DAOS_HL_ARRAY_DKEY_BLOCK_LEN");
	if (dkey_block_len_val) {
		sscanf(dkey_block_len_val, "%zu", &dkey_block_len_g);

		printf("DAOS_HL_ARRAY_DKEY_BLOCK_LEN = %s %zu\n",
		       dkey_block_len_val, dkey_block_len_g);
	}
	else {
		dkey_block_len_g = DAOS_HL_DKEY_BLOCK_SIZE;
	}

	dkey_num_blocks_val = getenv ("DAOS_HL_ARRAY_DKEY_NUM_BLOCKS");
	if (dkey_num_blocks_val) {
		sscanf(dkey_num_blocks_val, "%zu", &dkey_num_blocks_g);

		printf("DAOS_HL_ARRAY_DKEY_NUM_BLOCKS = %s %zu\n",
		       dkey_num_blocks_val, dkey_num_blocks_g);
	}
	else {
		dkey_num_blocks_g = DAOS_HL_DKEY_NUM_BLOCKS;
	}

	num_dkeys_val = getenv ("DAOS_HL_ARRAY_NUM_DKEYS");
	if (num_dkeys_val) {
		sscanf(num_dkeys_val, "%zu", &num_dkeys_g);

		printf("DAOS_HL_ARRAY_NUM_DKEYS = %s %zu\n",
		       num_dkeys_val, num_dkeys_g);
	}
	else {
		num_dkeys_g = DAOS_HL_DKEY_NUM;
	}
}
#endif

static int
daos_hl_extent_same(daos_hl_array_ranges_t *ranges, daos_sg_list_t *sgl)
{
	daos_size_t ranges_len;
	daos_size_t sgl_len;
	daos_size_t u;

	ranges_len = 0;
	printf("USER ARRAY RANGE -----------------------\n");
	printf("ranges_nr = %zu\n", ranges->ranges_nr);
	for (u = 0 ; u < ranges->ranges_nr ; u++) {
		ranges_len += ranges->ranges[u].len;
		printf("%zu: length %zu, index %d\n",
			u, ranges->ranges[u].len, (int)ranges->ranges[u].index);
	}
	printf("------------------------------------\n");

	sgl_len = 0;
	printf("USER SGL -----------------------\n");
	printf("sg_nr = %u\n", sgl->sg_nr.num);
	for (u = 0 ; u < sgl->sg_nr.num; u++) {
		sgl_len += sgl->sg_iovs[u].iov_len;
		printf("%zu: length %zu, Buf %p\n",
			u, sgl->sg_iovs[u].iov_len, sgl->sg_iovs[u].iov_buf);
	}
	printf("------------------------------------\n");
	return ((ranges_len == sgl_len) ? 1 : 0);
}

static int
compute_dkey(daos_off_t array_i, daos_size_t *num_records, daos_off_t *record_i,
	     char **dkey_str)
{
	daos_off_t 	byte_a; 	/* Byte address of I/O */
	daos_size_t 	dkey_grp; 	/* Which grp of dkeys to look into */
	daos_off_t 	dkey_grp_a; 	/* Byte address of dkey_grp */
	daos_off_t 	rel_byte_a; 	/* offset relative to grp */
	daos_size_t 	dkey_num; 	/* The dkey number for access */
	daos_size_t 	grp_iter; 	/* round robin iteration number */
	daos_off_t	dkey_byte_a;	/* address of dkey relative to group */

	byte_a = array_i * DAOS_HL_CELL_SIZE;

	/* Compute dkey group number and address */
	dkey_grp = byte_a / DAOS_HL_DKEY_GRP_SIZE;
	dkey_grp_a = dkey_grp * DAOS_HL_DKEY_GRP_SIZE;
	//printf("byte_a %d\n", (int)byte_a);
	//printf("dkey_grp %zu\n", dkey_grp);
	//printf("dkey_grp_a %d\n", (int)dkey_grp_a);

	/* Compute dkey number within dkey group */
	rel_byte_a = byte_a - dkey_grp_a;
	dkey_num = (size_t)(rel_byte_a / DAOS_HL_DKEY_BLOCK_SIZE) %
		DAOS_HL_DKEY_NUM;
	//printf("rel_byte_a %d\n", (int)rel_byte_a);
	//printf("dkey_num %zu\n", dkey_num);

	/* Compute relative offset/index in dkey */
	grp_iter = rel_byte_a / DAOS_HL_DKEY_GRP_CHUNK;
	dkey_byte_a = (grp_iter * DAOS_HL_DKEY_GRP_CHUNK) +
		(dkey_num * DAOS_HL_DKEY_BLOCK_SIZE);
	*record_i = (DAOS_HL_DKEY_BLOCK_SIZE * grp_iter) +
		(rel_byte_a - dkey_byte_a);
	//printf("grp_iter %zu\n", grp_iter);
	//printf("dkey_byte_a %d\n", (int)dkey_byte_a);
	//printf("record_i %d\n", (int)(*record_i));

	/* Number of records to access in current dkey */
	*num_records = ((grp_iter + 1) * DAOS_HL_DKEY_BLOCK_SIZE) - *record_i;
	//printf("num_records %zu\n", *num_records);

	asprintf(dkey_str, "%zu_%zu", dkey_grp, dkey_num);
	if (NULL == dkey_str) {
		D_ERROR("Failed memory allocation\n");
		return -1;
	}
	//printf("DKEY for range = %s\n", *dkey_str);
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
	sgl->sg_iovs = NULL;
	rem_records = num_records;

	/* 
	 * Keep iterating through the user sgl till we populate our sgl to
	 * satisfy the number of records to read/write from the KV object
	 */
	do {
		D_ASSERT(user_sgl->sg_nr.num > cur_i);

		sgl->sg_nr.num ++;
		sgl->sg_iovs = (daos_iov_t *)realloc(sgl->sg_iovs,
						     sizeof(daos_iov_t) *
						     sgl->sg_nr.num);
		if(NULL == sgl->sg_iovs) {
			D_ERROR("Failed memory allocation\n");
			return -1;
		}

		sgl->sg_iovs[k].iov_buf = user_sgl->sg_iovs[cur_i].iov_buf +
			cur_off;

		if (rem_records >= (user_sgl->sg_iovs[cur_i].iov_len - cur_off)) {
			sgl->sg_iovs[k].iov_len = user_sgl->sg_iovs[cur_i].iov_len - cur_off;
			cur_i ++;
			cur_off = 0;
		}
		else {
			sgl->sg_iovs[k].iov_len = rem_records;
			cur_off += rem_records;
		}

		sgl->sg_iovs[k].iov_buf_len = sgl->sg_iovs[k].iov_len;
		rem_records -= sgl->sg_iovs[k].iov_len;

		k ++;
	} while (rem_records && user_sgl->sg_nr.num > cur_i);

	sgl->sg_nr.num_out = 0;

	*sgl_i = cur_i;
	*sgl_off = cur_off;

	return 0;
}

static int
daos_hl_access_obj(daos_handle_t oh, daos_epoch_t epoch,
		   daos_hl_array_ranges_t *ranges, daos_sg_list_t *user_sgl,
		   daos_csum_buf_t *csums, daos_event_t *ev,
		   daos_hl_op_type_t op_type)
{
	daos_off_t 	cur_off;/* offset into user buffer sgl to track current
				 * position */
	daos_size_t 	cur_i; 	/* index into user sgl iovec to track current
				 * position */
	daos_size_t 	records;	/* Number of records to read/write in
					 * current range */
	daos_off_t 	array_i;	/* object array index of current
					 * range */
	daos_size_t	u;
	int		rc;
	daos_size_t	num_records;
	daos_off_t 	record_i;
	daos_csum_buf_t	null_csum;

	if (NULL == ranges) {
		D_ERROR("NULL ranges passed\n");
		return -1;
	}
	if (NULL == user_sgl) {
		D_ERROR("NULL scatter-gather list passed\n");
		return -1;
	}

	rc = daos_hl_extent_same(ranges, user_sgl);
	if (1 != rc) {
		D_ERROR("Unequal extents of memory and array descriptors\n");
		return rc;
	}

#if 0
	rc = daos_hl_parse_env_vars();
	if (0 != rc) {
		D_ERROR("Array read failed (%d)\n", rc);
		return rc;
	}
#endif

	daos_csum_set(&null_csum, NULL, 0);

	cur_off = 0;
	cur_i = 0;
	u = 0;
	records = ranges->ranges[0].len;
	array_i = ranges->ranges[0].index;

	/** 
	 * Loop over every range, but at the same time combine consecutive
	 * ranges that belong to the same dkey. If the user gives ranges that
	 * are not increasing in offset, they probably won't be combined unless
	 * the separating ranges also belong to the same dkey.
	 */
	while(u < ranges->ranges_nr) {
		daos_vec_iod_t 	iod;
		daos_sg_list_t 	sgl;
		char		*dkey_str = NULL;
		daos_key_t 	dkey;
		char		akey[] = "akey_not_used";
		bool		user_sgl_used = false;
		daos_size_t	dkey_records;
		daos_size_t	i;

		if (0 == ranges->ranges[u].len) {
			u ++;
			if(u < ranges->ranges_nr) {
				records = ranges->ranges[u].len;
				array_i = ranges->ranges[u].index;
			}
			continue;
		}

		/** 
		 * Compute the dkey given the array index for this range. Also
		 * compute: - the number of records that the dkey can hold
		 * starting at the index where we start writing. - the record
		 * index relative to the dkey.
		 */
		rc = compute_dkey(array_i, &num_records, &record_i, &dkey_str);
		if (rc != 0) {
			D_ERROR("Failed to compute dkey\n");
			return rc;
		}
		printf("DKEY IOD %s ---------------------------\n", dkey_str);
		//printf("array_i = %d\t num_records = %zu\t record_i = %d\n",
		//(int)array_i , num_records, (int)record_i);
		daos_iov_set(&dkey, (void *)dkey_str, strlen(dkey_str));

		/* set descriptor for KV object */
		daos_iov_set(&iod.vd_name, (void *)akey, strlen(akey));
		iod.vd_kcsum = null_csum;
		iod.vd_nr = 0;
		iod.vd_csums = NULL;
		iod.vd_eprs = NULL;
		iod.vd_recxs = NULL;
		i = 0;
		dkey_records = 0;

		/**
		 * Create the IO descriptor for this dkey. If the entire range
		 * fits in the dkey, continue to the next range to see if we can
		 * combine it fully or partially in the current dkey IOD/
		 */
		do {
			daos_off_t 	old_array_i;

			iod.vd_nr ++;

			/** add another element to recxs */
			iod.vd_recxs = (daos_recx_t *)realloc
				(iod.vd_recxs, sizeof(daos_recx_t) * iod.vd_nr);
			if (NULL == iod.vd_recxs) {
				D_ERROR("Failed memory allocation\n");
				return -1;
			}

			/** set the record access for this range */
			iod.vd_recxs[i].rx_rsize = 1;
			iod.vd_recxs[i].rx_idx = record_i;
			iod.vd_recxs[i].rx_nr = (num_records > records) ? 
				records : num_records;
			printf("Adding Vector %zu to ARRAY IOD (size = %zu, index = %d)\n",
			       u, iod.vd_recxs[i].rx_nr, (int)iod.vd_recxs[i].rx_idx);

			/** 
			 * if the current range is bigger than what the dkey can
			 * hold, update the array index and number of records in
			 * the current range and break to issue the I/O on the
			 * current KV.
			 */
			if(records > num_records) {
				array_i += num_records;
				records -= num_records;
				dkey_records += num_records;
				break;
			}

			u ++;
			i ++;
			dkey_records += records;

			/** if there are no more ranges to write, then break */
			if(ranges->ranges_nr <= u)
				break;

			old_array_i = array_i;
			records = ranges->ranges[u].len;
			array_i = ranges->ranges[u].index;

			/** 
			 * Boundary case where number of records align with the
			 * end boundary of the KV. break after advancing to the
			 * next range
			 */
			if(records == num_records)
				break;

			/** continue processing the next range in the current dkey */
			if(array_i < old_array_i + num_records &&
			   array_i >= ((old_array_i + num_records) - 
				       DAOS_HL_DKEY_BLOCK_SIZE)) {
				char	*dkey_str_tmp = NULL;

				/** 
				 * verify that the dkey is the same as the one
				 * we are working on given the array index, and
				 * also compute the number of records left in
				 * the dkey and the record indexin the dkey.
				 */
				rc = compute_dkey(array_i, &num_records, &record_i,
						  &dkey_str_tmp);
				if (rc != 0) {
					D_ERROR("Failed to compute dkey\n");
					return rc;
				}

				D_ASSERT(0 == strcmp(dkey_str_tmp, dkey_str));

				free(dkey_str_tmp);
				dkey_str_tmp = NULL;
			}
			else {
				break;
			}
		} while(1);
		printf("END DKEY IOD %s ---------------------------\n", dkey_str);

		/** 
		 * if the user sgl maps directly to the array range, no need to
		 * partition it.
		 */
		if (1 == ranges->ranges_nr && 1 == user_sgl->sg_nr.num &&
			dkey_records == ranges->ranges[0].len) {
			sgl = *user_sgl;
			user_sgl_used = true;
		}
		/** create an sgl from the user sgl for the current IOD */
		else {
			daos_size_t s;

			/* set sgl for current dkey */
			rc = create_sgl(user_sgl, dkey_records, &cur_off, 
					&cur_i, &sgl);
			if (rc != 0) {
				D_ERROR("Failed to create sgl\n");
				return rc;
			}
			printf("DKEY SGL -----------------------\n");
			printf("sg_nr = %u\n", sgl.sg_nr.num);
			for (s = 0; s < sgl.sg_nr.num; s++) {
				printf("%zu: length %zu, Buf %p\n",
				       s, sgl.sg_iovs[s].iov_len, sgl.sg_iovs[s].iov_buf);
			}
			printf("------------------------------------\n");
		}

		/* issue KV IO to DAOS */
		if(DAOS_HL_OP_READ == op_type) {
			rc = daos_obj_fetch(oh, epoch, &dkey, 1, &iod, 
					    &sgl, NULL, NULL);
			if (rc != 0) {
				D_ERROR("KV Fetch of dkey %s failed (%d)\n", 
					dkey_str, rc);
				return rc;
			}
		}
		else if(DAOS_HL_OP_WRITE == op_type) {
			rc = daos_obj_update(oh, epoch, &dkey, 1, &iod, 
					     &sgl, NULL);
			if (rc != 0) {
				D_ERROR("KV Update of dkey %s failed (%d)\n", 
					dkey_str, rc);
				return rc;
			}
		}
		else {
			D_ASSERTF(0, "Invalid array operation.\n");
		}

		if (dkey_str) {
			free(dkey_str);
			dkey_str = NULL;
		}

		if(!user_sgl_used && sgl.sg_iovs) {
			free(sgl.sg_iovs);
			sgl.sg_iovs = NULL;
		}

		if(iod.vd_recxs) {
			free(iod.vd_recxs);
			iod.vd_recxs = NULL;
		}
	} /* end while */

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
		D_ERROR("Array read failed (%d)\n", rc);
		return rc;
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
		D_ERROR("Array write failed (%d)\n", rc);
		return rc;
	}
	
	return rc;
}

#define ENUM_KEY_BUF	32
#define ENUM_DESC_BUF	512
#define ENUM_DESC_NR	5

int
daos_hl_array_get_size(daos_handle_t oh, daos_epoch_t epoch, daos_size_t *size,
		       daos_event_t *ev)
{
	uint32_t	key_nr, i, j;
	daos_sg_list_t  sgl;
	daos_hash_out_t hash_out;
	char		key[ENUM_KEY_BUF];
	daos_key_desc_t kds[ENUM_DESC_NR];
	daos_size_t 	len = ENUM_DESC_BUF;
	char		*ptr;
	char		*buf;
	uint32_t	max_hi, max_lo;
	daos_off_t 	max_offset;
	daos_size_t	max_iter;
	int 		rc;

	memset(&hash_out, 0, sizeof(hash_out));
	buf = malloc(ENUM_DESC_BUF);
	if (NULL == buf) {
		D_ERROR("Failed memory allocation\n");
		return -1;
	}

	max_hi = 0;
	max_lo = 0;

	/** enumerate records */
	for (i = ENUM_DESC_NR, key_nr = 0; !daos_hash_is_eof(&hash_out);
	     i = ENUM_DESC_NR) {
		daos_iov_t 	iov;

		memset(buf, 0, ENUM_DESC_BUF);

		sgl.sg_nr.num = 1;
		daos_iov_set(&iov, buf, len);
		sgl.sg_iovs = &iov;

		rc = daos_obj_list_dkey(oh, epoch, &i, kds, &sgl, &hash_out, NULL);
		if (0 != rc) {
			D_ERROR("DKey list failed (%d)\n", rc);
			return rc;
		}

		if (i == 0)
			continue; /* loop should break for EOF */

		key_nr += i;
		for (ptr = buf, j = 0; j < i; j++) {
			uint32_t hi, lo;

			snprintf(key, kds[j].kd_key_len + 1, ptr);
			printf("%d: key %s len %d\n", j, key,
				      (int)kds[j].kd_key_len);

			/** Keep a record of the highest dkey */
			sscanf(key, "%u_%u", &hi, &lo);
			if(hi >= max_hi) {
				max_hi = hi;
				if(lo > max_lo)
					max_lo = lo;
			}
			ptr += kds[j].kd_key_len;
		}
	}

	printf("MAX DKEY = (%u %u)\n", max_hi, max_lo);

	/* 
	 * Go through all the dkeys in the current group (maxhi_x) and get the
	 * highest index to determine which dkey in the group has the highest
	 * bit.
	 */
	max_iter = 0;
	max_offset = 0;

	for (i = 0 ; i <= max_lo; i++) {
		daos_off_t offset, index_hi = 0;
		daos_size_t iter;

		sprintf(key, "%u_%u", max_hi, i);
		printf("checking offset in dkey %s\n", key);
		/** retrieve the highest index */
		/** MSC - need new functionality from DAOS to retrieve that. */

		/** Compute the iteration where the highest record is stored */
		iter = index_hi / DAOS_HL_DKEY_BLOCK_SIZE;

		offset = iter * DAOS_HL_DKEY_GRP_CHUNK + 
			(index_hi - iter * DAOS_HL_DKEY_BLOCK_SIZE);

		if (iter == max_iter || max_iter == 0) {
			//D_ASSERT(offset > max_offset);
			max_offset = offset;
			max_iter = iter;
		}
		else {
			if (i < max_lo)
				break;
		}
	}

	*size = max_hi * DAOS_HL_DKEY_GRP_SIZE + max_offset;

	free(buf);
	buf = NULL;

	return rc;
}
