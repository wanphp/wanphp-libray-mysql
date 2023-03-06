<?php

namespace Wanphp\Libray\Mysql;

use Exception;

abstract class BaseRepository implements BaseInterface
{
  protected Database $db;
  protected string $tableName;
  private string $entityClass;

  public function __construct(Database $database, string $tableName, string $entityClass)
  {
    $this->db = $database;
    $this->tableName = $tableName;
    $this->entityClass = $entityClass;
  }

  /**
   * {@inheritDoc}
   */
  public function insert(array $data): int
  {
    if (!isset($data[0])) $data = [$data];
    $required = $this->required();
    foreach ($data as &$item) $this->checkedData($item, $required);

    $this->db->insert($this->tableName, $data);
    return $this->returnResult($this->db->id() ?? 0);
  }

  /**
   * {@inheritDoc}
   */
  public function update(array $data, array $where): int
  {
    $this->checkedData($data, []);//$this->required() 更新数据时不做数据完整性检测
    if (empty($data)) throw new Exception('更新数据不能为空！');
    $res = $this->db->update($this->tableName, $data, $where);
    if ($res) $counts = $res->rowCount();
    return $this->returnResult($counts ?? 0);
  }

  /**
   * {@inheritDoc}
   */
  public function select(string $columns = '*', $where = null): array
  {
    if ($columns != '*' && strpos($columns, ',') > 0) $columns = explode(',', $columns);
    $data = $this->db->select($this->tableName, $columns, $where);
    return $this->returnResult($data ?? []);
  }

  /**
   * {@inheritDoc}
   */
  public function get(string $columns = '*', $where = null)
  {
    if ($columns != '*' && strpos($columns, ',') > 0) $columns = explode(',', $columns);
    $data = $this->db->get($this->tableName, $columns, $where);
    return $this->returnResult($data ?? []);
  }

  /**
   * {@inheritDoc}
   */
  public function count(string $columns = '*', $where = null): int
  {
    return $this->db->count($this->tableName, $columns, $where);
  }

  /**
   * {@inheritDoc}
   */
  public function sum(string $column, $where = null): ?string
  {
    return $this->db->sum($this->tableName, $column, $where);
  }

  /**
   * {@inheritDoc}
   */
  public function delete(array $where): int
  {
    $res = $this->db->delete($this->tableName, $where);
    if ($res) $counts = $res->rowCount();
    return $this->returnResult($counts ?? 0);
  }

  /**
   * {@inheritDoc}
   */
  public function log()
  {
    $logs = implode(PHP_EOL, $this->db->log());
    throw new Exception($logs ?: '无数据库操作！');
  }

  /**
   * @return array
   * @throws Exception
   */
  private function required(): array
  {
    $required = [];//必须项
    try {
      $class = new \ReflectionClass($this->entityClass); //建立实体类的反射类
      $docblock = $class->getDocComment();
      if (preg_match('/required={(.*?)}/', $docblock, $primary)) {
        $property = json_decode("[{$primary[1]}]");
        if (json_last_error() !== JSON_ERROR_NONE) throw new Exception('必须项JSON格式错误.');
        foreach ($property as $item) {
          if ($class->hasProperty($item)) {
            //属性存在，取属性描述
            $docblock = $class->getProperty($item)->getDocComment();
            if (preg_match('/Property\(description="(.*?)"\)/', $docblock, $primary)) {
              $required[$item] = $primary[1];
            }
          }
        }
      }
    } catch (\ReflectionException $exception) {
      throw new Exception($exception->getMessage(), $exception->getCode());
    }
    return $required;
  }

  /**
   * @param $data
   * @param $required
   * @throws Exception
   */
  private function checkedData(&$data, $required)
  {
    $data = array_filter((new $this->entityClass($data))->jsonSerialize(), function ($value, $key) use ($required) {
      if (!empty($required) && array_key_exists($key, $required) && $value == '') {
        throw new Exception($required[$key] . ' - 不能为空');
      }
      return !is_null($value);
    }, ARRAY_FILTER_USE_BOTH);

    $res = [];
    foreach ($data as $key => $value) {
      if (is_array($value)) $key .= '[JSON]';
      $res[$key] = $value;
    }
    $data = $res;
  }

  /**
   * @param $result
   * @throws Exception
   */
  private function returnResult($result)
  {
    $error = $this->db->errorInfo;
    if (is_null($error)) return $result;
    //数据表不存在，或字段不存在，主键冲突,创建或更新表
    if (is_array($error) && in_array($error[1], [1146, 1054, 1062])) {
      $this->db->initTable($this->tableName, $this->entityClass);
    }

    throw new Exception($error[2], $error[1]);
  }
}
