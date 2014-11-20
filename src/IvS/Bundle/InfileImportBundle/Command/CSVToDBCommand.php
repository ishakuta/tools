<?php

namespace IvS\Bundle\InfileImportBundle\Command;

use Doctrine\DBAL\Connection;

use Psr\Log\LoggerInterface;

use Symfony\Component\Finder\Finder;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

use Oro\Component\Log\OutputLogger;

use IvS\Bundle\InfileImportBundle\Service\CsvFileReader;

class CSVToDBCommand extends ContainerAwareCommand
{
    const SOURCE_ARGUMENT = 'source';

    /**
     * {@inheritdoc}
     */
    protected function configure()
    {
        $this->setName('iv:import:csv');
        $this->addArgument(
            self::SOURCE_ARGUMENT,
            InputArgument::REQUIRED,
            'Path to extracted sales force data files'
        );
    }

    /**
     * {@inheritdoc}
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $directory          = $input->getArgument(self::SOURCE_ARGUMENT);
        $logger             = new OutputLogger($output);


        // get file list
        $finder = new Finder();
        $finder->files()
            ->in($directory)
            ->name('*.csv');

        /** @var \SplFileInfo $csvFile */
        foreach ($finder as $csvFile) {
            $fileName = $csvFile->getPathname();
            $reader = new CsvFileReader($fileName, ['escape' => '"']);
            $record = $reader->read();

            if (empty($record)) {
                continue;
            }

            $logger->info(
                sprintf(
                    '    > <info>%s</info>, fields found: %d',
                    $csvFile->getPathname(),
                    count($record)
                )
            );

            $tableName  = 'CS_' . str_replace([$csvFile->getPath(), DIRECTORY_SEPARATOR, '.csv'], '', $fileName);
            if (!$this->isTablesExists($tableName)) {
                $this->isTablesExists($tableName);

                $fieldTypes = $this->guessFieldsLength($fileName, $tableName);

                $this->createTable($tableName, $fieldTypes, $logger);
            }

            $this->loadData(realpath($fileName), $tableName);
        }

        $logger->info('END');
    }

    /**
     * @param string $table
     *
     * @return bool
     */
    protected function isTablesExists($table)
    {
        /** @var Connection $connection */
        $connection = $this->getContainer()->get('database_connection');
        $tableName  = $connection->fetchColumn(sprintf('SHOW TABLES LIKE \'%s\';', $table));

        return $table == $tableName;
    }

    protected function getSpecialTables()
    {
        return [];
    }

    protected function getCount($table)
    {
        /** @var Connection $connection */
        $connection = $this->getContainer()->get('database_connection');
        $count = $connection->fetchColumn(sprintf('SELECT COUNT(*) FROM \'%s\';', $table));

        return (int)$count;
    }

    /**
     * @param string $file
     * @param string $table
     */
    protected function loadData($file, $table)
    {
        if (in_array($table, $this->getSpecialTables())) {
            $query = sprintf(
                "LOAD DATA INFILE '%s' INTO TABLE %s " .
                "FIELDS TERMINATED BY ',' ENCLOSED BY '%s' ESCAPED BY '%s' IGNORE 1 LINES;",
                $file,
                $table,
                '"',
                '"'
            );
        } else {
            $query = sprintf(
                "LOAD DATA INFILE '%s' INTO TABLE %s " .
                "FIELDS TERMINATED BY ',' ENCLOSED BY '%s' IGNORE 1 LINES;",
                $file,
                $table,
                '"'
            );
        }

        /** @var Connection $connection */
        $connection = $this->getContainer()->get('database_connection');
        $connection->executeQuery($query);
    }

    /**
     * Create tables based on guessed field types
     *
     * @param string          $tableName
     * @param array           $fields
     * @param LoggerInterface $logger
     */
    protected function createTable($tableName, array $fields, LoggerInterface $logger)
    {
        $migration = new CreateTableMigration($tableName, $fields);

        $executor = $this->getContainer()->get('oro_migration.migrations.executor');
        $executor->setLogger($logger);
        $executor->getQueryExecutor()->setLogger($logger);

        $executor->executeUp([$migration]);
    }

    /**
     * Parse file values and return assoc array with max length for each field
     * Used to create varchar or text columns later
     *
     * @param string $file
     * @param string $tableName
     *
     * @return array
     */
    protected function guessFieldsLength($file, $tableName)
    {
        if (in_array($tableName, $this->getSpecialTables())) {
            $options = ['escape' => '""'];
        } else {
            $options = [];
        }

        $reader = new CsvFileReader($file, $options);
        $metadata = [];

        $processCallback = function ($proceedRecords) {
            fwrite(STDOUT, "\r".str_pad($proceedRecords . ' record processed', 50));
        };

        $proceedRecords = 0;
        while ($record = $reader->read()) {
            foreach ($record as $fieldName => $fieldValue) {
                $length = mb_strlen($fieldValue);
                $metadata[$fieldName] = isset($metadata[$fieldName]) ? $metadata[$fieldName] : 0;
                $metadata[$fieldName] = $length > $metadata[$fieldName] ? $length : $metadata[$fieldName];
            }

            $processCallback(++$proceedRecords);
        }
        fwrite(STDOUT, "\r\n");

        $fieldTypes = [];
        foreach ($metadata as $fieldName => $length) {
            $type = (count($fieldTypes) > 5) ? 'text' : ($length > 255 ? 'text' : 'string');
            $fieldTypes[$fieldName] = $type;
        }

        return $fieldTypes;
    }
}
