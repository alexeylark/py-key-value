import asyncio
import logging
from typing import overload

from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.stores.base import (
    BaseContextManagerStore,
    BaseStore,
    BasicSerializationAdapter,
)

try:
    from google.cloud import firestore, firestore_admin_v1
    from google.cloud.firestore_admin_v1.services.firestore_admin.async_client import FirestoreAdminAsyncClient
    from google.oauth2.service_account import Credentials
except ImportError as e:
    msg = "FirestoreStore requires py-key-value-aio[firestore]"
    raise ImportError(msg) from e

logger = logging.getLogger(__name__)


class FirestoreStore(BaseContextManagerStore, BaseStore):
    """Firestore-based key-value store.

    This store uses Firebase DB as the key-value storage.
    The data is stored in collections.
    """

    _client: firestore.AsyncClient | None

    @overload
    def __init__(self, client: firestore.AsyncClient, *, default_collection: str | None = None) -> None:
        """Initialize the Firestore store with a client. It defers project and database from client instance.

        Args:
            client: The initialized Firestore client to use.
            default_collection: The default collection to use if no collection is provided.
        """

    @overload
    def __init__(
        self, *, credentials: Credentials, project: str | None = None, database: str | None = None, default_collection: str | None = None
    ) -> None:
        """Initialize the Firestore store with Google service account credentials.

        Args:
            credentials: Google service account credentials from google-cloud-auth module.
            project: Google project name.
            database: database name, defaults to '(default)' if not provided.
            default_collection: The default collection to use if no collection is provided.
        """

    def __init__(
        self,
        client: firestore.AsyncClient | None = None,
        *,
        credentials: Credentials | None = None,
        project: str | None = None,
        database: str | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the Firestore store with Google client or Google service account credentials.
        If provided with a client, uses it, otherwise connects using credentials.

        Args:
            client: The initialized Firestore client to use. Chosen by default if provided.
            credentials: Google service account credentials from google-cloud-auth module.
            project: Google project name.
            database: database name, defaults to '(default)' if not provided.
            default_collection: The default collection to use if no collection is provided.
        """
        self._credentials = credentials
        self._project = project
        self._database = database
        serialization_adapter = BasicSerializationAdapter(value_format="string", date_format="datetime")

        if client:
            self._client = client
            client_provided_by_user = True
        else:
            self._client = firestore.AsyncClient(credentials=self._credentials, project=self._project, database=self._database)
            client_provided_by_user = False
        super().__init__(
            default_collection=default_collection,
            client_provided_by_user=client_provided_by_user,
            serialization_adapter=serialization_adapter,
        )

    @property
    def _connected_client(self) -> firestore.AsyncClient:
        if not self._client:
            msg = "Client not connected"
            raise ValueError(msg)
        return self._client

    @override
    async def _get_managed_entry(self, *, key: str, collection: str | None = None) -> ManagedEntry | None:
        """Get a managed entry from Firestore."""
        collection = collection or self.default_collection
        response = await self._connected_client.collection(collection).document(key).get()  # pyright: ignore[reportUnknownMemberType]
        doc = response.to_dict()
        if doc is None:
            return None
        return self._serialization_adapter.load_dict(data=doc)

    @override
    async def _put_managed_entry(self, *, key: str, managed_entry: ManagedEntry, collection: str | None = None) -> None:
        """Store a managed entry in Firestore."""
        collection = collection or self.default_collection
        item = self._serialization_adapter.dump_dict(entry=managed_entry)
        await self._connected_client.collection(collection).document(key).set(item)  # pyright: ignore[reportUnknownMemberType]

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str | None = None) -> bool:
        """Delete a managed entry from Firestore."""
        collection = collection or self.default_collection
        await self._connected_client.collection(collection).document(key).delete()
        return True

    @override
    async def _setup(self) -> None:
        # Don't need to create indexes in the test client
        if type(self._client).__name__== "InMemoryAsyncFirestoreClient":
            return

        # Enable TTL, disable indexes on every field
        # This can be done asynchroneously, so that the app starts quickly
        async def setup_indexes():
            async with FirestoreAdminAsyncClient(
                credentials=self._client._credentials,
                client_info=self._client._client_info,
                client_options=self._client._client_options,
            ) as admin_client:

                def update_field_done_callback(name, future: asyncio.Future):
                    try:
                        future.result()
                        logger.debug(f"{name} succeeded!")
                    except Exception:
                        logger.error(f"{name} failed", exc_info=True)

                logger.debug("Requesting global index suppression (this affects ALL collections)...")
                index_op = await admin_client.update_field(
                    firestore_admin_v1.UpdateFieldRequest(
                        field=firestore_admin_v1.types.Field(
                            name=f"projects/{self._client.project}/databases/{self._client._database}/collectionGroups/*/fields/*",
                            index_config=firestore_admin_v1.types.Field.IndexConfig(
                                indexes=[],  # Remove all single-field indexes
                                uses_ancestor_config=False,
                            ),
                        ),
                        update_mask={"paths": ["index_config"]},
                    )
                )
                index_op.add_done_callback(lambda result: update_field_done_callback("Global index suppression", result))

                logger.debug("Enabling TTL on expires_at...")
                ttl_op = await admin_client.update_field(
                    firestore_admin_v1.UpdateFieldRequest(
                        field=firestore_admin_v1.types.Field(
                            name=f"projects/{self._client.project}/databases/{self._client._database}/collectionGroups/*/fields/expires_at",
                            ttl_config=firestore_admin_v1.types.Field.TtlConfig(),
                        ),
                        update_mask={"paths": ["ttl_config"]},
                    )
                )
                ttl_op.add_done_callback(lambda result: update_field_done_callback("TTL on expire", result))

        asyncio.get_event_loop().create_task(setup_indexes())

    async def _close(self) -> None:
        """Close the Firestore client."""
        if self._client and not self._client_provided_by_user:
            self._client.close()
