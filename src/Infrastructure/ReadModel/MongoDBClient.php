<?php declare(strict_types=1);

namespace SaaSFormation\Framework\MongoDBBasedReadModel\Infrastructure\ReadModel;

use MongoDB\Client;
use MongoDB\Driver\ReadConcern;
use MongoDB\Driver\ReadPreference;
use MongoDB\Driver\Session;
use MongoDB\Driver\WriteConcern;

readonly class MongoDBClient
{
    private Session $session;

    public function __construct(private Client $client)
    {
        $this->session = $this->client->startSession();
    }

    public function beginTransaction(): void
    {
        $transactionOptions = [
            'readConcern' => new ReadConcern(ReadConcern::LOCAL),
            'writeConcern' => new WriteConcern(WriteConcern::MAJORITY),
            'readPreference' => new ReadPreference(ReadPreference::RP_PRIMARY),
        ];

        $this->session->startTransaction($transactionOptions);
    }

    public function commit(): void
    {
        $this->session->commitTransaction();
    }

    public function rollback(): void
    {
        $this->session->abortTransaction();
    }

    /**
     * @param string $databaseName
     * @param array<string, string> $options
     * @return MongoDBDatabase
     */
    public function selectDatabase(string $databaseName, array $options = []): MongoDBDatabase
    {
        return (new MongoDBDatabase($this->client->selectDatabase($databaseName, $options), $this->session));
    }
}