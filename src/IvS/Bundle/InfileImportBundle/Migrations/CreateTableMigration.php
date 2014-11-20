<?php

namespace IvS\Bundle\InfileImportBundle\Migrations;

use Doctrine\DBAL\Schema\Schema;

use Oro\Bundle\MigrationBundle\Migration\QueryBag;
use Oro\Bundle\MigrationBundle\Migration\Migration;

class CreateTableMigration implements Migration
{
    /** @var string */
    protected $tableName;

    /** @var array */
    protected $fields;

    /**
     * @param string $tableName
     * @param array  $fields
     */
    public function __construct($tableName, $fields)
    {
        $this->tableName = $tableName;
        $this->fields    = $fields;
    }

    /**
     * {@inheritdoc}
     */
    public function up(Schema $schema, QueryBag $queries)
    {
        if (empty($this->tableName) || empty($this->fields)) {
            throw new \Exception('No table or columns defined!');
        }

        $table = $schema->createTable($this->tableName);

        foreach ($this->fields as $fieldName => $fieldType) {
            $table->addColumn($fieldName, $fieldType);
        }
    }
}
