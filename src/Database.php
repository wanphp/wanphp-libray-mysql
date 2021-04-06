<?php

namespace Wanphp\Libray\Mysql;


use Medoo\Medoo;

class Database extends Medoo
{
  /**
   * @param string $table 表名
   * @param string $classcame 实体
   * @throws \Exception
   */
  public function initTable(string $table, string $classcame)
  {
    if ($this->logging) {
      $tableName = $this->prefix . $table;
      $stack = [];

      //源表结构
      $source = ['fields' => [], 'pri' => []];
      try {
        $query = $this->exec("DESC `{$tableName}`");
        while ($row = $query->fetch(\PDO::FETCH_ASSOC)) {
          $source['fields'][] = $row['Field'];
          if ($row['Key'] == 'PRI') $source['pri'][] = $row['Field'];
          if ($row['Key'] == 'UNI') $source['uni'][] = $row['Field'];
          if ($row['Key'] == 'MUL') $source['mul'][] = $row['Field'];
        }
        //print_r($source);
      } catch (\Exception $e) {
      }
      $pri = [];//主键字段
      $uni = [];//唯一索引
      $mul = [];//普通索引
      $after = '';

      try {
        $class = new \ReflectionClass($classcame); //建立实体类的反射类
        $properties = $class->getProperties(\ReflectionProperty::IS_PRIVATE);
        foreach ($properties as $property) {
          $docblock = $property->getDocComment();//取成员变量属性
          if (preg_match('/\@DBType\(\{(.*?)\}\)/', $docblock, $primary)) {
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
        throw new \Exception($exception->getMessage(), $exception->getCode());
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
    }
  }
}
