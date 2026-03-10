/*
 * kv_bench - SPDK bdev benchmark for Mooncake Store KV Cache workload
 *
 * Simulates the offload path: sequential write of multiple KV objects,
 * then sequential read back, measuring latency and throughput.
 *
 * Usage: sudo ./kv_bench --json /tmp/spdk_bdev.json -b Malloc0 [-n 16] [-s 1048576]
 *   -b <bdev>   block device name (required)
 *   -n <count>  number of KV objects to write/read (default: 16)
 *   -s <bytes>  size of each KV object in bytes (default: 1MB)
 */

#include "spdk/stdinc.h"
#include "spdk/thread.h"
#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/log.h"
#include "spdk/string.h"

static char *g_bdev_name = "Malloc0";
static int g_num_objects = 16;
static int g_object_size = 1048576; /* 1MB default */

struct bench_context {
	struct spdk_bdev *bdev;
	struct spdk_bdev_desc *bdev_desc;
	struct spdk_io_channel *bdev_io_channel;
	char *buff;
	uint32_t buff_size;
	uint32_t block_size;
	struct spdk_bdev_io_wait_entry bdev_io_wait;

	int num_objects;
	int object_size;
	int current_index;

	struct timespec phase_start;
	struct timespec op_start;
	double *write_latencies_us;
	double *read_latencies_us;
};

static void bench_read_next(void *arg);

static double
timespec_diff_us(struct timespec *start, struct timespec *end)
{
	return (end->tv_sec - start->tv_sec) * 1e6 +
	       (end->tv_nsec - start->tv_nsec) / 1e3;
}

static void
print_results(struct bench_context *ctx)
{
	int i;
	double total_write_us = 0, total_read_us = 0;
	double min_w = 1e18, max_w = 0, min_r = 1e18, max_r = 0;
	double total_bytes = (double)ctx->num_objects * ctx->object_size;

	for (i = 0; i < ctx->num_objects; i++) {
		total_write_us += ctx->write_latencies_us[i];
		total_read_us += ctx->read_latencies_us[i];
		if (ctx->write_latencies_us[i] < min_w) min_w = ctx->write_latencies_us[i];
		if (ctx->write_latencies_us[i] > max_w) max_w = ctx->write_latencies_us[i];
		if (ctx->read_latencies_us[i] < min_r) min_r = ctx->read_latencies_us[i];
		if (ctx->read_latencies_us[i] > max_r) max_r = ctx->read_latencies_us[i];
	}

	printf("\n===== KV Bench Results =====\n");
	printf("Objects: %d x %d bytes = %.2f MB total\n",
	       ctx->num_objects, ctx->object_size, total_bytes / (1024.0 * 1024.0));
	printf("Block size: %u bytes\n", ctx->block_size);
	printf("\n--- Write ---\n");
	printf("  Total:   %.2f ms\n", total_write_us / 1000.0);
	printf("  Avg:     %.2f us/op\n", total_write_us / ctx->num_objects);
	printf("  Min:     %.2f us\n", min_w);
	printf("  Max:     %.2f us\n", max_w);
	printf("  Throughput: %.2f MB/s\n",
	       total_bytes / (total_write_us / 1e6) / (1024.0 * 1024.0));
	printf("\n--- Read ---\n");
	printf("  Total:   %.2f ms\n", total_read_us / 1000.0);
	printf("  Avg:     %.2f us/op\n", total_read_us / ctx->num_objects);
	printf("  Min:     %.2f us\n", min_r);
	printf("  Max:     %.2f us\n", max_r);
	printf("  Throughput: %.2f MB/s\n",
	       total_bytes / (total_read_us / 1e6) / (1024.0 * 1024.0));
	printf("============================\n\n");
}

static void
bench_cleanup(struct bench_context *ctx, int status)
{
	if (status == 0) {
		print_results(ctx);
	}
	free(ctx->write_latencies_us);
	free(ctx->read_latencies_us);
	spdk_put_io_channel(ctx->bdev_io_channel);
	spdk_bdev_close(ctx->bdev_desc);
	spdk_app_stop(status);
}

/* ---- Read path ---- */

static void
read_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct bench_context *ctx = cb_arg;
	struct timespec now;

	spdk_bdev_free_io(bdev_io);

	if (!success) {
		SPDK_ERRLOG("Read failed at object %d\n", ctx->current_index);
		bench_cleanup(ctx, -1);
		return;
	}

	clock_gettime(CLOCK_MONOTONIC, &now);
	ctx->read_latencies_us[ctx->current_index] =
		timespec_diff_us(&ctx->op_start, &now);

	/* Verify first 8 bytes contain the expected pattern */
	uint64_t expected = (uint64_t)ctx->current_index;
	uint64_t actual;
	memcpy(&actual, ctx->buff, sizeof(actual));
	if (actual != expected) {
		SPDK_ERRLOG("Data mismatch at object %d: expected %lu, got %lu\n",
			    ctx->current_index, expected, actual);
		bench_cleanup(ctx, -1);
		return;
	}

	ctx->current_index++;
	if (ctx->current_index < ctx->num_objects) {
		bench_read_next(ctx);
	} else {
		SPDK_NOTICELOG("All %d reads completed and verified\n", ctx->num_objects);
		bench_cleanup(ctx, 0);
	}
}

static void
bench_read_next(void *arg)
{
	struct bench_context *ctx = arg;
	uint64_t offset = (uint64_t)ctx->current_index * ctx->buff_size;
	int rc;

	memset(ctx->buff, 0, ctx->buff_size);
	clock_gettime(CLOCK_MONOTONIC, &ctx->op_start);

	rc = spdk_bdev_read(ctx->bdev_desc, ctx->bdev_io_channel,
			    ctx->buff, offset, ctx->buff_size,
			    read_complete, ctx);
	if (rc == -ENOMEM) {
		ctx->bdev_io_wait.bdev = ctx->bdev;
		ctx->bdev_io_wait.cb_fn = bench_read_next;
		ctx->bdev_io_wait.cb_arg = ctx;
		spdk_bdev_queue_io_wait(ctx->bdev, ctx->bdev_io_channel,
					&ctx->bdev_io_wait);
	} else if (rc) {
		SPDK_ERRLOG("Read submit failed: %d\n", rc);
		bench_cleanup(ctx, -1);
	}
}

/* ---- Write path ---- */

static void write_next(void *arg);

static void
write_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct bench_context *ctx = cb_arg;
	struct timespec now;

	spdk_bdev_free_io(bdev_io);

	if (!success) {
		SPDK_ERRLOG("Write failed at object %d\n", ctx->current_index);
		bench_cleanup(ctx, -1);
		return;
	}

	clock_gettime(CLOCK_MONOTONIC, &now);
	ctx->write_latencies_us[ctx->current_index] =
		timespec_diff_us(&ctx->op_start, &now);

	ctx->current_index++;
	if (ctx->current_index < ctx->num_objects) {
		write_next(ctx);
	} else {
		SPDK_NOTICELOG("All %d writes completed, starting reads\n",
			       ctx->num_objects);
		ctx->current_index = 0;
		bench_read_next(ctx);
	}
}

static void
write_next(void *arg)
{
	struct bench_context *ctx = arg;
	uint64_t offset = (uint64_t)ctx->current_index * ctx->buff_size;
	int rc;

	/* Fill buffer: first 8 bytes = object index, rest = pattern */
	uint64_t pattern = (uint64_t)ctx->current_index;
	memcpy(ctx->buff, &pattern, sizeof(pattern));
	memset(ctx->buff + sizeof(pattern), 'A' + (ctx->current_index % 26),
	       ctx->buff_size - sizeof(pattern));

	clock_gettime(CLOCK_MONOTONIC, &ctx->op_start);

	rc = spdk_bdev_write(ctx->bdev_desc, ctx->bdev_io_channel,
			     ctx->buff, offset, ctx->buff_size,
			     write_complete, ctx);
	if (rc == -ENOMEM) {
		ctx->bdev_io_wait.bdev = ctx->bdev;
		ctx->bdev_io_wait.cb_fn = write_next;
		ctx->bdev_io_wait.cb_arg = ctx;
		spdk_bdev_queue_io_wait(ctx->bdev, ctx->bdev_io_channel,
					&ctx->bdev_io_wait);
	} else if (rc) {
		SPDK_ERRLOG("Write submit failed: %d\n", rc);
		bench_cleanup(ctx, -1);
	}
}

/* ---- Entry point ---- */

static void
bench_bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
		    void *event_ctx)
{
	SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
}

static void
bench_start(void *arg)
{
	struct bench_context *ctx = arg;
	uint32_t buf_align;
	int rc;

	SPDK_NOTICELOG("Starting KV bench: %d objects x %d bytes\n",
		       ctx->num_objects, ctx->object_size);

	rc = spdk_bdev_open_ext(g_bdev_name, true, bench_bdev_event_cb, NULL,
				&ctx->bdev_desc);
	if (rc) {
		SPDK_ERRLOG("Could not open bdev: %s\n", g_bdev_name);
		spdk_app_stop(-1);
		return;
	}

	ctx->bdev = spdk_bdev_desc_get_bdev(ctx->bdev_desc);
	ctx->block_size = spdk_bdev_get_block_size(ctx->bdev);

	ctx->bdev_io_channel = spdk_bdev_get_io_channel(ctx->bdev_desc);
	if (!ctx->bdev_io_channel) {
		SPDK_ERRLOG("Could not create I/O channel\n");
		spdk_bdev_close(ctx->bdev_desc);
		spdk_app_stop(-1);
		return;
	}

	/* Round object_size up to block_size alignment */
	ctx->buff_size = (ctx->object_size + ctx->block_size - 1) &
			 ~(ctx->block_size - 1);

	buf_align = spdk_bdev_get_buf_align(ctx->bdev);
	ctx->buff = spdk_dma_zmalloc(ctx->buff_size, buf_align, NULL);
	if (!ctx->buff) {
		SPDK_ERRLOG("Failed to allocate %u byte DMA buffer\n", ctx->buff_size);
		spdk_put_io_channel(ctx->bdev_io_channel);
		spdk_bdev_close(ctx->bdev_desc);
		spdk_app_stop(-1);
		return;
	}

	/* Verify device has enough space */
	uint64_t device_size = spdk_bdev_get_num_blocks(ctx->bdev) * ctx->block_size;
	uint64_t required_size = (uint64_t)ctx->num_objects * ctx->buff_size;
	if (required_size > device_size) {
		SPDK_ERRLOG("Device too small: need %lu bytes, have %lu bytes\n",
			    required_size, device_size);
		spdk_dma_free(ctx->buff);
		spdk_put_io_channel(ctx->bdev_io_channel);
		spdk_bdev_close(ctx->bdev_desc);
		spdk_app_stop(-1);
		return;
	}

	ctx->write_latencies_us = calloc(ctx->num_objects, sizeof(double));
	ctx->read_latencies_us = calloc(ctx->num_objects, sizeof(double));

	SPDK_NOTICELOG("Device: %s, block_size=%u, buff_size=%u, total=%lu bytes\n",
		       g_bdev_name, ctx->block_size, ctx->buff_size, required_size);

	ctx->current_index = 0;
	write_next(ctx);
}

static void
kv_bench_usage(void)
{
	printf(" -b <bdev>   block device name (required)\n");
	printf(" -N <count>  number of KV objects (default: 16)\n");
	printf(" -S <bytes>  size of each object (default: 1048576 = 1MB)\n");
}

static int
kv_bench_parse_arg(int ch, char *arg)
{
	switch (ch) {
	case 'b':
		g_bdev_name = arg;
		break;
	case 'N':
		g_num_objects = atoi(arg);
		if (g_num_objects <= 0) {
			fprintf(stderr, "Invalid object count: %s\n", arg);
			return -EINVAL;
		}
		break;
	case 'S':
		g_object_size = atoi(arg);
		if (g_object_size <= 0) {
			fprintf(stderr, "Invalid object size: %s\n", arg);
			return -EINVAL;
		}
		break;
	default:
		return -EINVAL;
	}
	return 0;
}

int
main(int argc, char **argv)
{
	struct spdk_app_opts opts = {};
	struct bench_context ctx = {};
	int rc;

	spdk_app_opts_init(&opts, sizeof(opts));
	opts.name = "kv_bench";
	opts.rpc_addr = NULL;

	rc = spdk_app_parse_args(argc, argv, &opts, "b:N:S:", NULL,
				 kv_bench_parse_arg, kv_bench_usage);
	if (rc != SPDK_APP_PARSE_ARGS_SUCCESS) {
		exit(rc);
	}

	ctx.num_objects = g_num_objects;
	ctx.object_size = g_object_size;

	rc = spdk_app_start(&opts, bench_start, &ctx);
	if (rc) {
		SPDK_ERRLOG("ERROR starting application\n");
	}

	spdk_dma_free(ctx.buff);
	spdk_app_fini();
	return rc;
}
