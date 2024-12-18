<?php declare(strict_types=1);

namespace SaaSFormation\Framework\MongoDBBasedReadModel\Infrastructure\ReadModel;

use Assert\Assert;
use Psr\Log\LoggerInterface;
use SaaSFormation\Framework\SharedKernel\Application\Messages\QueryInterface;
use SaaSFormation\Framework\SharedKernel\Application\ReadModel\AbstractReadModel;
use SaaSFormation\Framework\SharedKernel\Application\ReadModel\ReadModelRepositoryInterface;
use SaaSFormation\Framework\SharedKernel\Application\ReadModel\RepositoryCollectionResult;
use SaaSFormation\Framework\SharedKernel\Common\Identity\IdInterface;
use SaaSFormation\Framework\SharedKernel\Common\Identity\UUIDFactoryInterface;
use SaaSFormation\Framework\SharedKernel\Domain\Messages\DomainEventInterface;

readonly abstract class MongoDBBasedReadModelRepository implements ReadModelRepositoryInterface
{
    private MongoDBClient $client;

    public function __construct(private MongoDBClientProvider $mongoDBClientProvider, private LoggerInterface $logger, private UUIDFactoryInterface $uuidFactory)
    {
        $this->client = $this->mongoDBClientProvider->provide();
    }

    public function save(DomainEventInterface $domainEvent, AbstractReadModel $readModel): void
    {
        $this->logger->debug("Trying to save a read model", ['read_model_code' => $readModel->code()]);

        $id = $readModel->getReadModelId();
        if(!$id) {
            $readModel->setReadModelId($id = $this->uuidFactory->generate());
        }

        $data['data'] = $readModel->toArray();
        $data['_id'] = $id->humanReadable();

        Assert::that($domainEvent->getRequestId())->isInstanceOf(IdInterface::class, "Request id is null at MongoDBBasedReadModelRepository save");
        $this->client
            ->selectDatabase($domainEvent->getRequestId(), $this->databaseName())
            ->selectCollection($this->collectionName())
            ->updateOne(['_id' => $data['_id']], ['$set' => ['data' => $data['data']]], ['upsert' => true]);

        $this->logger->debug("Read model was saved.", ['read_model_code' => $readModel->code()]);
    }

    public function findOneByCriteria(QueryInterface $query, array $criteria): ?AbstractReadModel
    {
        $this->logger->debug("Trying to find one read model", ['criteria' => $criteria]);
        $result = $this->findByCriteria($query, $criteria);

        if($result->totalResultsRetrieved === 0) {
            $this->logger->warning("Read model not found", ['criteria' => $criteria]);
            throw new \Exception("No results found for the given criteria.");
        }

        $this->logger->debug("One read model found", ['code' => $result->readModels[0]->code(), 'criteria' => $criteria]);
        return $result->readModels[0];
    }

    public function findByCriteria(QueryInterface $query, array $criteria): RepositoryCollectionResult
    {
        $this->logger->debug("Trying to find read models", ['criteria' => $criteria]);
        $readModels = [];

        Assert::that($query->getRequestId())->isInstanceOf(IdInterface::class, "Request id is null at MongoDBBasedReadModelRepository findByCriteria");
        $totalResults = $this->client
            ->selectDatabase($query->getRequestId(), $this->databaseName())
            ->selectCollection($this->collectionName())
            ->countDocuments();

        $results = $this->client
            ->selectDatabase($query->getRequestId(), $this->databaseName())
            ->selectCollection($this->collectionName())
            ->find($criteria);

        $results->setTypeMap(['root' => 'array', 'document' => 'array', 'array' => 'array']);

        $items = $results->toArray();

        foreach ($items as $result) {
            $className = $this->readModelClass();
            $readModels[] = $className::fromArray($result['_id'], $result['data']);
        }

        $this->logger->debug("Read models found", ['total' => count($readModels), 'criteria' => $criteria]);

        return new RepositoryCollectionResult($totalResults, $readModels);
    }

    public abstract function databaseName(): string;
    public abstract function collectionName(): string;
    public abstract function readModelClass(): string;
}