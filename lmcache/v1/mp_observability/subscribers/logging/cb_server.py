# SPDX-License-Identifier: Apache-2.0

"""Blend logging subscriber — debug logs for cache blending events."""

# Future
from __future__ import annotations

# First Party
from lmcache.logging import init_logger
from lmcache.v1.mp_observability.event import Event, EventType
from lmcache.v1.mp_observability.event_bus import EventCallback, EventSubscriber

logger = init_logger(__name__)


class BlendLoggingSubscriber(EventSubscriber):
    """Logs cache blending (CB) events at debug level.

    Covers the top-level lookup/retrieve/store events and, for V3, the
    lookup and retrieve sub-phases (fingerprint match, prefix leg, coordinator
    match, sparse prefetch, scatter) plus no-op retrieves.
    """

    def get_subscriptions(self) -> dict[EventType, EventCallback]:
        """Return the mapping of event types to handler callbacks."""
        return {
            EventType.CB_STORE_PRE_COMPUTED_START: self._on_store_pre_start,
            EventType.CB_STORE_PRE_COMPUTED_END: self._on_store_pre_end,
            EventType.CB_LOOKUP_START: self._on_lookup_start,
            EventType.CB_LOOKUP_END: self._on_lookup_end,
            EventType.CB_RETRIEVE_START: self._on_retrieve_start,
            EventType.CB_RETRIEVE_END: self._on_retrieve_end,
            EventType.CB_STORE_FINAL_START: self._on_store_final_start,
            EventType.CB_STORE_FINAL_END: self._on_store_final_end,
            EventType.CB_FINGERPRINTS_REGISTERED: self._on_fingerprints_registered,
            EventType.CB_CHUNKS_EVICTED: self._on_chunks_evicted,
            # V3 lookup / retrieve sub-phases (END only — the START carries no
            # payload the log line would add to).
            EventType.CB_FINGERPRINT_MATCH_END: self._on_fingerprint_match_end,
            EventType.CB_PREFIX_LOOKUP_END: self._on_prefix_lookup_end,
            EventType.CB_COORDINATOR_MATCH_END: self._on_coordinator_match_end,
            EventType.CB_SPARSE_PREFETCH_START: self._on_sparse_prefetch_start,
            EventType.CB_SPARSE_PREFETCH_END: self._on_sparse_prefetch_end,
            EventType.CB_SCATTER_START: self._on_scatter_start,
            EventType.CB_RETRIEVE_NOOP: self._on_retrieve_noop,
        }

    def _on_store_pre_start(self, event: Event) -> None:
        logger.debug(
            "CB store_pre_computed start: session=%s instance_id=%s num_tokens=%s",
            event.session_id,
            event.metadata.get("instance_id"),
            event.metadata.get("num_tokens"),
        )

    def _on_store_pre_end(self, event: Event) -> None:
        logger.debug(
            "CB store_pre_computed end: session=%s instance_id=%s"
            " num_tokens=%s stored_chunks=%s success=%s",
            event.session_id,
            event.metadata.get("instance_id"),
            event.metadata.get("num_tokens"),
            event.metadata.get("stored_chunks"),
            event.metadata.get("success"),
        )

    def _on_lookup_start(self, event: Event) -> None:
        logger.debug(
            "CB lookup start: session=%s num_tokens=%s",
            event.session_id,
            event.metadata.get("num_tokens"),
        )

    def _on_lookup_end(self, event: Event) -> None:
        logger.debug(
            "CB lookup end: session=%s num_tokens=%s"
            " fingerprint_hits=%s storage_hits=%s stale_chunks=%s no_gpu_context=%s",
            event.session_id,
            event.metadata.get("num_tokens"),
            event.metadata.get("fingerprint_hits"),
            event.metadata.get("storage_hits"),
            event.metadata.get("stale_chunks"),
            event.metadata.get("no_gpu_context"),
        )

    def _on_retrieve_start(self, event: Event) -> None:
        logger.debug(
            "CB retrieve start: session=%s instance_id=%s num_chunks=%s",
            event.session_id,
            event.metadata.get("instance_id"),
            event.metadata.get("num_chunks"),
        )

    def _on_retrieve_end(self, event: Event) -> None:
        logger.debug(
            "CB retrieve end: session=%s instance_id=%s num_chunks=%s success=%s",
            event.session_id,
            event.metadata.get("instance_id"),
            event.metadata.get("num_chunks"),
            event.metadata.get("success"),
        )

    def _on_store_final_start(self, event: Event) -> None:
        logger.debug(
            "CB store_final start: session=%s instance_id=%s num_tokens=%s",
            event.session_id,
            event.metadata.get("instance_id"),
            event.metadata.get("num_tokens"),
        )

    def _on_store_final_end(self, event: Event) -> None:
        logger.debug(
            "CB store_final end: session=%s instance_id=%s"
            " num_tokens=%s stored_chunks=%s success=%s",
            event.session_id,
            event.metadata.get("instance_id"),
            event.metadata.get("num_tokens"),
            event.metadata.get("stored_chunks"),
            event.metadata.get("success"),
        )

    def _on_fingerprints_registered(self, event: Event) -> None:
        logger.debug(
            "CB fingerprint table: +%s chunks (%s tokens)",
            event.metadata.get("num_chunks"),
            event.metadata.get("num_tokens"),
        )

    def _on_chunks_evicted(self, event: Event) -> None:
        logger.debug(
            "CB fingerprint table: evicted %s stale chunks",
            event.metadata.get("num_chunks"),
        )

    def _on_fingerprint_match_end(self, event: Event) -> None:
        logger.debug(
            "CB fingerprint match: session=%s matches=%s",
            event.session_id,
            event.metadata.get("matches"),
        )

    def _on_prefix_lookup_end(self, event: Event) -> None:
        logger.debug(
            "CB prefix lookup end: session=%s prefix_chunks=%s",
            event.session_id,
            event.metadata.get("prefix_chunks"),
        )

    def _on_coordinator_match_end(self, event: Event) -> None:
        logger.debug(
            "CB coordinator match end: session=%s matches=%s timed_out=%s",
            event.session_id,
            event.metadata.get("matches"),
            event.metadata.get("timed_out"),
        )

    def _on_sparse_prefetch_start(self, event: Event) -> None:
        logger.debug(
            "CB sparse prefetch start: session=%s n_chunks=%s n_keys=%s l2_keys=%s",
            event.session_id,
            event.metadata.get("n_chunks"),
            event.metadata.get("n_keys"),
            event.metadata.get("l2_keys"),
        )

    def _on_sparse_prefetch_end(self, event: Event) -> None:
        logger.debug(
            "CB sparse prefetch end: session=%s found_keys=%s of l2_keys=%s",
            event.session_id,
            event.metadata.get("found_keys"),
            event.metadata.get("l2_keys"),
        )

    def _on_scatter_start(self, event: Event) -> None:
        logger.debug(
            "CB scatter start: session=%s scattered_tokens=%s"
            " n_prefix=%s n_shifted=%s dropped=%s",
            event.session_id,
            event.metadata.get("scattered_tokens"),
            event.metadata.get("n_prefix"),
            event.metadata.get("n_shifted"),
            event.metadata.get("dropped"),
        )

    def _on_retrieve_noop(self, event: Event) -> None:
        logger.debug(
            "CB retrieve no-op: session=%s reason=%s dropped_matches=%s",
            event.session_id,
            event.metadata.get("reason"),
            event.metadata.get("dropped_matches"),
        )
