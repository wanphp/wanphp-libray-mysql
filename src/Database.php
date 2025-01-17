<?php

namespace Wanphp\Libray\Mysql;


use Exception;
use Medoo\Medoo;

class Database extends Medoo
{
  /**
   * 设置通过前缀创建使用分表
   * @param string $prefix
   * @return void
   */
  public function setPrefix(string $prefix): void
  {
    if (!str_contains($this->prefix, $prefix)) $this->prefix .= $prefix;
  }

  /**
   * @param string $table 表名
   * @param string $classname 实体
   * @throws Exception
   */
  public function initTable(string $table, string $classname): void
  {
    if ($this->logging) {
      $tableName = $this->prefix . $table;
      $stack = [];

      //源表结构
      $source = ['fields' => [], 'pri' => []];
      // 表存在更新表
      if ($this->type === 'mysql') {
        if (is_array($this->errorInfo) && $this->errorInfo[1] != 1146) {
          $query = $this->exec("DESC `{$tableName}`");
          if ($query) while ($row = $query->fetch(\PDO::FETCH_ASSOC)) {
            $source['fields'][] = $row['Field'];
            if ($row['Key'] == 'PRI') $source['pri'][] = $row['Field'];
            if ($row['Key'] == 'UNI') $source['uni'][] = $row['Field'];
            if ($row['Key'] == 'MUL') $source['mul'][] = $row['Field'];
          }
        }
        $pri = [];//主键字段
        $uni = [];//唯一索引
        $mul = [];//普通索引
        $after = '';

        try {
          $class = new \ReflectionClass($classname); //建立实体类的反射类
          $properties = $class->getProperties(\ReflectionProperty::IS_PRIVATE | \ReflectionProperty::IS_PROTECTED);
          foreach ($properties as $property) {
            $docblock = $property->getDocComment();//取成员变量属性
            if (preg_match('/DBType\({(.*?)}\)/', $docblock, $primary)) {
              $dbtype = json_decode("{{$primary[1]}}");
              if (json_last_error() === JSON_ERROR_NONE) {
                if (isset($dbtype->key)) {
                  if ($dbtype->key == 'PRI') $pri[] = $property->getName();
                  if ($dbtype->key == 'UNI') $uni[] = $property->getName();
                  if ($dbtype->key == 'MUL') $mul[] = $property->getName();
                }
                if (empty($source['fields'])) {//数据库无表
                  $stack[] = '`' . $property->getName() . '` ' . $dbtype->type;
                } else if (!in_array($property->getName(), $source['fields'])) {//添加字段
                  $stack[] = 'ADD COLUMN `' . $property->getName() . '` ' . $dbtype->type . (empty($after) ? ' FIRST' : ' AFTER `' . $after . '`');
                } else {//修改字段
                  $stack[] = 'MODIFY COLUMN `' . $property->getName() . '` ' . $dbtype->type . (empty($after) ? ' FIRST' : ' AFTER `' . $after . '`');
                }
                $after = $property->getName();
              }
            }
          }
        } catch (\ReflectionException $exception) {
          throw new Exception($exception->getMessage(), $exception->getCode());
        }
        if (!empty($stack)) {
          if (empty($source['fields'])) {//数据库无表
            if (!empty($pri)) $stack[] = 'PRIMARY KEY (`' . join('`,`', $pri) . '`)';
            if (!empty($uni)) foreach ($uni as $value) $stack[] = 'UNIQUE KEY `KEY_' . strtoupper($value) . '` (`' . $value . '`) USING BTREE';
            if (!empty($mul)) foreach ($mul as $value) $stack[] = 'INDEX `IDX_' . strtoupper($value) . '` (`' . $value . '`) USING BTREE';
            $this->query('CREATE TABLE IF NOT EXISTS `' . $tableName . '` (' . join(',', $stack) . ');');
          } else {
            if (!empty($pri) && isset($source['pri']) && !empty(array_diff($source['pri'], $pri))) $stack[] = 'DROP PRIMARY KEY,ADD PRIMARY KEY (`' . join('`,`', $pri) . '`)';
            if (!empty($uni)) foreach ($uni as $value) {//新增唯一索引
              if (!isset($source['uni']) || !in_array($value, $source['uni'])) $stack[] = 'ADD UNIQUE KEY `KEY_' . strtoupper($value) . '` (`' . $value . '`) USING BTREE';
            }
            if (isset($source['uni'])) foreach ($source['uni'] as $value) {//删除唯一索引
              if (empty($uni) || !in_array($value, $uni)) $stack[] = 'DROP INDEX `KEY_' . strtoupper($value) . '`';
            }
            if (!empty($mul)) foreach ($mul as $value) {//新增普通索引
              if (!isset($source['mul']) || !in_array($value, $source['mul'])) $stack[] = 'ADD INDEX `IDX_' . strtoupper($value) . '` (`' . $value . '`) USING BTREE';
            }
            if (isset($source['mul'])) foreach ($source['mul'] as $value) {//删除普通索引
              if (empty($mul) || !in_array($value, $mul)) $stack[] = 'DROP INDEX `IDX_' . strtoupper($value) . '`';
            }

            $this->query('ALTER TABLE `' . $tableName . '` ' . join(',', $stack) . ';');
          }
        }
      } elseif ($this->type === 'sqlite') {
        if (is_array($this->errorInfo) && $this->errorInfo[1] != 1) { // SQLite 错误代码 1：表不存在
          $query = $this->query("PRAGMA table_info(`{$tableName}`)");
          if ($query) while ($row = $query->fetch(\PDO::FETCH_ASSOC)) {
            $source['fields'][] = $row['name'];
            if ($row['pk'] == 1) $source['pri'][] = $row['name'];
          }
        }

        $pri = [];
        $uni = [];
        $mul = [];
        $createTable = [];

        try {
          $class = new \ReflectionClass($classname);
          $properties = $class->getProperties(\ReflectionProperty::IS_PRIVATE | \ReflectionProperty::IS_PROTECTED);
          foreach ($properties as $property) {
            $docblock = $property->getDocComment();
            if (preg_match('/DBType\({(.*?)}\)/', $docblock, $primary)) {
              $dbtype = json_decode("{{$primary[1]}}");
              if (json_last_error() === JSON_ERROR_NONE) {
                if (isset($dbtype->key)) {
                  if ($dbtype->key == 'PRI' && !str_contains($dbtype->type, 'AUTO_INCREMENT')) $pri[] = $property->getName();
                  if ($dbtype->key == 'UNI') $uni[] = $property->getName();
                  if ($dbtype->key == 'MUL') $mul[] = $property->getName();
                }

                if (empty($source['fields'])) { // 表不存在，创建表
                  if (str_contains($dbtype->type, 'AUTO_INCREMENT')) $dbtype->type = 'INTEGER PRIMARY KEY AUTOINCREMENT';
                  $stack[] = '`' . $property->getName() . '` ' . $dbtype->type;
                } else {
                  if (!in_array($property->getName(), $source['fields'])) { // 添加字段
                    $stack[] = 'ALTER TABLE `' . $tableName . '` ADD COLUMN `' . $property->getName() . '` ' . $dbtype->type;
                  } else {
                    // 更新主键，需要重新创建表
                    if (!empty($pri) && (empty($source['pri']) ||
                        ((count(array_diff($pri, $source['pri'])) > 0 || count(array_diff($source['pri'], $pri)) > 0))
                      )
                    ) {
                      if (str_contains($dbtype->type, 'AUTO_INCREMENT')) $dbtype->type = 'INTEGER PRIMARY KEY AUTOINCREMENT';
                      $createTable[] = '`' . $property->getName() . '` ' . $dbtype->type;
                    }
                  }
                }
              }
            }
          }
        } catch (\ReflectionException $exception) {
          throw new Exception($exception->getMessage(), $exception->getCode());
        }

        if (!empty($stack)) {
          if (empty($source['fields'])) { // 表不存在时创建
            //SQLite 的 PRIMARY KEY 不支持 AUTOINCREMENT 和复合主键同时存在
            if (!empty($pri) && !str_contains(join(',', $stack), 'PRIMARY KEY')) $stack[] = 'PRIMARY KEY (`' . join('`,`', $pri) . '`)';
            $this->query('CREATE TABLE IF NOT EXISTS `' . $tableName . '` (' . join(',', $stack) . ');');
            // 添加唯一索引
            if (!empty($uni)) foreach ($uni as $value) {
              $this->query('CREATE UNIQUE INDEX `KEY_' . strtoupper($value) . '` ON `' . $tableName . '` (`' . $value . '`);');
            }
            // 添加普通索引
            if (!empty($mul)) foreach ($mul as $value) {
              $this->query('CREATE INDEXS `IDX_' . strtoupper($value) . '` ON `' . $tableName . '` (`' . $value . '`)');
            }
          } else { // 修改表结构
            // SQLite不支持直接修改主键
            if (!empty($createTable)) {
              if (!empty($pri) && !str_contains(join(',', $createTable), 'PRIMARY KEY')) $createTable[] = 'PRIMARY KEY (`' . join('`,`', $pri) . '`)';
              $this->action(function ($database) use ($tableName, $createTable) {
                // 1、先创建一个临时表
                $database->query('CREATE TABLE IF NOT EXISTS `' . $tableName . '_temp' . '` (' . join(',', $createTable) . ');');
                // 2、将数据导入临时表,如果数据结构不一样将可能导入失败或导入出错
                $database->query('INSERT INTO `' . $tableName . '_temp' . '`  SELECT * FROM `' . $tableName . '`;');
                // 3、删除旧表
                $database->query('DROP TABLE `' . $tableName . '`;');
                // 4、将临时表修改表名
                $database->query('ALTER TABLE `' . $tableName . '_temp' . '` RENAME TO `' . $tableName . '`;');
              });
            }
            // 删除和添加唯一索引
            if (!empty($uni)) foreach ($uni as $value) {//新增唯一索引
              $stack[] = 'CREATE UNIQUE INDEX IF NOT EXISTS `KEY_' . strtoupper($value) . '` ON `' . $tableName . '` (`' . $value . '`);';
            }
            if (isset($source['uni'])) foreach ($source['uni'] as $value) {// 删除不需要的唯一索引
              if (empty($uni) || !in_array($value, $uni)) $stack[] = 'DROP INDEX IF EXISTS `KEY_' . strtoupper($value) . '`;';
            }

            // 添加和删除普通索引
            if (!empty($mul)) foreach ($mul as $value) {
              $stack[] = 'CREATE INDEX IF NOT EXISTS `IDX_' . strtoupper($value) . '` ON `' . $tableName . '` (`' . $value . '`);';
            }
            if (isset($source['mul'])) foreach ($source['mul'] as $value) {//删除不需要的普通索引
              if (empty($mul) || !in_array($value, $mul)) $stack[] = 'DROP INDEX IF EXISTS `IDX_' . strtoupper($value) . '`;';
            }

            // 添加字段
            foreach ($stack as $command) {
              $this->query($command);
            }
          }
        }

      }
    }
  }
}
