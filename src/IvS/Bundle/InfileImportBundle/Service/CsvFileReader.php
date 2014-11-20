<?php

namespace IvS\Bundle\InfileImportBundle\Service;

// TODO: consider use this instead: 
// https://github.com/ddeboer/data-import/blob/master/src/Ddeboer/DataImport/Reader/CsvReader.php
class CsvFileReader
{
    /**
     * @var \SplFileInfo
     */
    protected $fileInfo;

    /**
     * @var \SplFileObject
     */
    protected $file;

    /**
     * @var string
     */
    protected $delimiter = ',';

    /**
     * @var string
     */
    protected $enclosure = '"';

    /**
     * @var string
     */
    protected $escape = '\\';

    /**
     * @var bool
     */
    protected $firstLineIsHeader = true;

    /**
     * @var array
     */
    protected $header;

    /**
     * @param string $filePath
     * @param array $options
     */
    public function __construct($filePath, array $options = [])
    {
        $this->setFilePath($filePath);
        $this->initialize($options);
    }

    /**
     * @param string $filePath
     * @throws \InvalidArgumentException
     */
    protected function setFilePath($filePath)
    {
        $this->fileInfo = new \SplFileInfo($filePath);

        if (!$this->fileInfo->isFile()) {
            throw new \InvalidArgumentException(sprintf('File "%s" does not exists.', $filePath));
        } elseif (!$this->fileInfo->isReadable()) {
            throw new \InvalidArgumentException(sprintf('File "%s" is not readable.', $this->fileInfo->getRealPath()));
        }
    }

    /**
     * @param array $options
     * @throws \InvalidArgumentException
     */
    protected function initialize(array $options)
    {
        if (isset($options['delimiter'])) {
            $this->delimiter = $options['delimiter'];
        }

        if (isset($options['enclosure'])) {
            $this->enclosure = $options['enclosure'];
        }

        if (isset($options['escape'])) {
            $this->escape = $options['escape'];
        }

        if (isset($options['firstLineIsHeader'])) {
            $this->firstLineIsHeader = (bool)$options['firstLineIsHeader'];
        }

        if (isset($options['header'])) {
            $this->header = $options['header'];
        }
    }

    /**
     * {@inheritdoc}
     */
    public function read()
    {
        if ($this->getFile()->eof()) {
            return null;
        }

        $data = $this->getFile()->fgetcsv();
        if (false !== $data) {
            if (null === $data || [null] === $data) {
                return $this->getFile()->eof() ? null : [];
            }

            if ($this->firstLineIsHeader) {
                if (count($this->header) !== count($data)) {
                    throw new \UnexpectedValueException(
                        sprintf(
                            'Expecting to get %d columns, actually got %d',
                            count($this->header),
                            count($data)
                        )
                    );
                }

                $data = array_combine($this->header, $data);
            }
        } else {
            throw new \RuntimeException('An error occurred while reading the csv.');
        }

        return $data;
    }

    /**
     * @return \SplFileObject
     */
    protected function getFile()
    {
        if (!$this->file instanceof \SplFileObject) {
            $this->file = $this->fileInfo->openFile();
            $this->file->setFlags(
                \SplFileObject::READ_CSV |
                \SplFileObject::READ_AHEAD |
                \SplFileObject::DROP_NEW_LINE
            );
            $this->file->setCsvControl(
                $this->delimiter,
                $this->enclosure,
                $this->escape
            );
            if ($this->firstLineIsHeader && !$this->header) {
                $this->header = $this->file->fgetcsv();
            }
        }

        return $this->file;
    }
}
