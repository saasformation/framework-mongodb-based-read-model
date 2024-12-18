<?php declare(strict_types=1);

namespace SaaSFormation\Framework\MongoDBBasedReadModel\Infrastructure\ReadModel;

use Assert\Assert;
use MongoDB\Client;
use MongoDB\Driver\ReadConcern;
use MongoDB\Driver\ReadPreference;
use MongoDB\Driver\Session;
use MongoDB\Driver\WriteConcern;
use SaaSFormation\Framework\SharedKernel\Common\Identity\IdInterface;

class MongoDBClient
{
    /** @var array<string, Session> */
    private array $sessions = [];
    /** @var array<string, int> */
    private array $transactionCounters = [];

    public function __construct(private readonly Client $client)
    {
    }

    public function startSession(IdInterface $requestId): Session
    {
        if(!isset($this->sessions[$requestId->humanReadable()])) {
            $this->sessions[$requestId->humanReadable()] = $this->client->startSession();
            $this->transactionCounters[$requestId->humanReadable()] = 0;
        }

        return $this->sessions[$requestId->humanReadable()];
    }

    public function endSession(IdInterface $requestId): void
    {
        $this->sessions[$requestId->humanReadable()]->endSession();
        unset($this->sessions[$requestId->humanReadable()]);
        unset($this->transactionCounters[$requestId->humanReadable()]);
    }

    public function hasSession(IdInterface $requestId): bool
    {
        return isset($this->sessions[$requestId->humanReadable()]);
    }

    public function beginTransaction(IdInterface $requestId): void
    {
        Assert::that($this->sessions)->keyIsset($requestId->humanReadable(), "Session is not set for id " . $requestId->humanReadable());
        Assert::that($this->transactionCounters)->keyIsset($requestId->humanReadable(), "Transactions counter is not ser for id " . $requestId->humanReadable());

        if ($this->transactionCounters[$requestId->humanReadable()] === 0) {
            $transactionOptions = [
                'readConcern' => new ReadConcern(ReadConcern::LOCAL),
                'writeConcern' => new WriteConcern(WriteConcern::MAJORITY),
                'readPreference' => new ReadPreference(ReadPreference::PRIMARY),
            ];

            $this->sessions[$requestId->humanReadable()]->startTransaction($transactionOptions);
        }

        $this->transactionCounters[$requestId->humanReadable()]++;
    }

    public function commitTransaction(IdInterface $requestId): void
    {
        Assert::that($this->sessions)->keyIsset($requestId->humanReadable(), "Session is not set for id " . $requestId->humanReadable());
        Assert::that($this->transactionCounters)->keyIsset($requestId->humanReadable(), "Transactions counter is not ser for id " . $requestId->humanReadable());

        if ($this->transactionCounters[$requestId->humanReadable()] === 0) {
            throw new \Exception("No active transaction to commit.");
        }

        $this->transactionCounters[$requestId->humanReadable()]--;

        if ($this->transactionCounters[$requestId->humanReadable()] === 0) {
            $this->sessions[$requestId->humanReadable()]->commitTransaction();
        }
    }

    public function rollbackTransaction(IdInterface $requestId): void
    {
        Assert::that($this->sessions)->keyIsset($requestId->humanReadable(), "Session is not set for id " . $requestId->humanReadable());
        Assert::that($this->transactionCounters)->keyIsset($requestId->humanReadable(), "Transactions counter is not ser for id " . $requestId->humanReadable());

        if ($this->transactionCounters[$requestId->humanReadable()] === 0) {
            throw new \Exception("No active transaction to rollback.");
        }

        $this->transactionCounters[$requestId->humanReadable()]--;

        if ($this->transactionCounters[$requestId->humanReadable()] === 0) {
            $this->sessions[$requestId->humanReadable()]->abortTransaction();
        }
    }

    /**
     * @param IdInterface $requestId
     * @param string $databaseName
     * @param array<string, string> $options
     * @return MongoDBDatabase
     */
    public function selectDatabase(IdInterface $requestId, string $databaseName, array $options = []): MongoDBDatabase
    {
        return (new MongoDBDatabase($this->client->selectDatabase($databaseName, $options), $this->sessions[$requestId->humanReadable()]));
    }
}