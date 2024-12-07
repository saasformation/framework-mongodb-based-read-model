<?php declare(strict_types=1);

namespace SaaSFormation\Framework\MongoDBBasedReadModel\Infrastructure\ReadModel;

use MongoDB\Client;
use SaaSFormation\Framework\Contracts\Infrastructure\EnvVarsManagerInterface;

class MongoDBClientProvider
{
    public function __construct(private EnvVarsManagerInterface $envVarsManager)
    {
    }

    public function provide(): Client
    {
        if(!is_string($this->envVarsManager->get('MONGODB_URI'))) {
            throw new \InvalidArgumentException('MONGODB_URI must be a string');
        }

        return new Client($this->envVarsManager->get('MONGODB_URI'));
    }
}