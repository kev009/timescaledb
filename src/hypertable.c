#include <postgres.h>
#include <access/htup_details.h>
#include <utils/lsyscache.h>
#include <nodes/memnodes.h>
#include <catalog/namespace.h>
#include <utils/memutils.h>

#include "hypertable.h"
#include "dimension.h"
#include "chunk.h"
#include "subspace_store.h"
#include "hypertable_cache.h"
#include "trigger.h"
#include "scanner.h"

Hypertable *
hypertable_from_tuple(HeapTuple tuple)
{
	Hypertable *h;
	Oid			namespace_oid;

	h = palloc0(sizeof(Hypertable));
	memcpy(&h->fd, GETSTRUCT(tuple), sizeof(FormData_hypertable));
	namespace_oid = get_namespace_oid(NameStr(h->fd.schema_name), false);
	h->main_table_relid = get_relname_relid(NameStr(h->fd.table_name), namespace_oid);
	h->space = dimension_scan(h->fd.id, h->main_table_relid, h->fd.num_dimensions);
	h->chunk_cache = subspace_store_init(h->space->num_dimensions, CurrentMemoryContext);

	return h;
}

typedef struct ChunkCacheEntry
{
	MemoryContext mcxt;
	Chunk	   *chunk;
} ChunkCacheEntry;

static void
chunk_cache_entry_free(void *cce)
{
	MemoryContextDelete(((ChunkCacheEntry *) cce)->mcxt);
}

Chunk *
hypertable_get_chunk(Hypertable *h, Point *point)
{
	ChunkCacheEntry *cce = subspace_store_get(h->chunk_cache, point);

	if (NULL == cce)
	{
		MemoryContext old_mcxt,
					chunk_mcxt;
		Chunk	   *chunk;

		/*
		 * chunk_find() must execute on a per-tuple memory context since it
		 * allocates a lot of transient data. We don't want this allocated on
		 * the cache's memory context.
		 */
		chunk = chunk_find(h->space, point);

		if (NULL == chunk)
			chunk = chunk_create(h, point,
								 NameStr(h->fd.associated_schema_name),
								 NameStr(h->fd.associated_table_prefix));

		Assert(chunk != NULL);

		chunk_mcxt = AllocSetContextCreate(subspace_store_mcxt(h->chunk_cache),
										   "chunk cache memory context",
										   ALLOCSET_SMALL_SIZES);

		old_mcxt = MemoryContextSwitchTo(chunk_mcxt);

		cce = palloc(sizeof(ChunkCacheEntry));
		cce->mcxt = chunk_mcxt;

		/* Make a copy which lives in the chunk cache's memory context */
		chunk = cce->chunk = chunk_copy(chunk);

		subspace_store_add(h->chunk_cache, chunk->cube, cce, chunk_cache_entry_free);
		MemoryContextSwitchTo(old_mcxt);
	}

	Assert(NULL != cce);
	Assert(NULL != cce->chunk);
	Assert(MemoryContextContains(cce->mcxt, cce));
	Assert(MemoryContextContains(cce->mcxt, cce->chunk));

	return cce->chunk;
}

static inline Oid
hypertable_relid_lookup(Oid relid)
{
	Cache	   *hcache = hypertable_cache_pin();
	Hypertable *ht = hypertable_cache_get_entry(hcache, relid);
	Oid			result = (ht == NULL) ? InvalidOid : ht->main_table_relid;

	cache_release(hcache);

	return result;
}

/*
 * Returns a hypertable's relation ID (OID) iff the given RangeVar corresponds to
 * a hypertable, otherwise InvalidOid.
*/
Oid
hypertable_relid(RangeVar *rv)
{
	return hypertable_relid_lookup(RangeVarGetRelid(rv, NoLock, true));
}

bool
is_hypertable(Oid relid)
{
	if (!OidIsValid(relid))
		return false;
	return hypertable_relid_lookup(relid) != InvalidOid;
}

PG_FUNCTION_INFO_V1(hypertable_validate_triggers);

Datum
hypertable_validate_triggers(PG_FUNCTION_ARGS)
{
	if (relation_has_transition_table_trigger(PG_GETARG_OID(0)))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		errmsg("Hypertables do not support transition tables in triggers.")));

	PG_RETURN_VOID();
}

static bool
hypertable_tuple_found(TupleInfo *ti, void *data)
{
	Hypertable **entry = data;

	*entry = hypertable_from_tuple(ti->tuple);
	return false;
}

Hypertable *
hypertable_get_by_id(int32 hypertable_id, bool fail_if_not_found)
{
	Catalog    *catalog = catalog_get();
	Hypertable *ptr = NULL;
	ScanKeyData scankey[1];
	ScannerCtx	scanCtx = {
		.table = catalog->tables[HYPERTABLE].id,
		.index = catalog->tables[HYPERTABLE].index_ids[HYPERTABLE_ID_INDEX],
		.scantype = ScannerTypeIndex,
		.nkeys = 1,
		.limit = 1,
		.scankey = scankey,
		.data = &ptr,
		.tuple_found = hypertable_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	/* Perform an index scan on hypertable_id. */
	ScanKeyInit(&scankey[0], Anum_hypertable_pkey_idx_id,
			  BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(hypertable_id));

	scanner_scan_one(&scanCtx, fail_if_not_found, "Hypertable");
	
	return ptr;
}
