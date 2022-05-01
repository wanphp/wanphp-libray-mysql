<?php

namespace Wanphp\Libray\Mysql;

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
    $required = $this->required();
    if (!isset($data[0])) $data = [$data];
    foreach ($data as &$item) $this->checkedData($item, $required);

    $this->db->insert($this->tableName, $data);
    return $this->returnResult($this->db->id() ?? 0);
  }

  /**
   * {@inheritDoc}
   */
  public function update(array $data, array $where): int
  {
    $this->checkedData($data, $this->required());
    $counts = $this->db->update($this->tableName, $data, $where)->rowCount();
    return $this->returnResult($counts ?? 0);
  }

  /**
   * {@inheritDoc}
   */
  public function select(string $columns = '*', array $where = null): array
  {
    if ($columns != '*' && strpos($columns, ',') > 0) $columns = explode(',', $columns);
    $data = $this->db->select($this->tableName, $columns, $where);
    return $this->returnResult($data ?? []);
  }

  /**
   * {@inheritDoc}
   */
  public function get(string $columns = '*', array $where = null)
  {
    if ($columns != '*' && strpos($columns, ',') > 0) $columns = explode(',', $columns);
    $data = $this->db->get($this->tableName, $columns, $where);
    return $this->returnResult($data ?? []);
  }

  /**
   * {@inheritDoc}
   */
  public function count(string $columns = '*', array $where = null): int
  {
    return $this->db->count($this->tableName, $columns, $where);
  }

  /**
   * {@inheritDoc}
   */
  public function sum(string $column, array $where = null): float
  {
    $total = $this->db->sum($this->tableName, $column, $where);
    return $total ?? 0.00;
  }

  /**
   * {@inheritDoc}
   */
  public function delete(array $where): int
  {
    $counts = $this->db->delete($this->tableName, $where)->rowCount();
    return $this->returnResult($counts ?? 0);
  }

  /**
   * {@inheritDoc}
   */
  public function log()
  {
    $logs = implode(PHP_EOL, $this->db->log());
    throw new \Exception($logs ?: '无数据库操作！');
  }

  /**
   * @return array|false|string[]
   * @throws \Exception
   */
  private function required()
  {
    $required = [];//必须项
    try {
      $class = new \ReflectionClass($this->entityClass); //建立实体类的反射类
      $docblock = $class->getDocComment();
      if (preg_match('/required=\{(.*?)\}/', $docblock, $primary)) {
        $required = explode(',', str_replace(['"', '\''], '', $primary[1]));
      }
    } catch (\ReflectionException $exception) {
      throw new \Exception($exception->getMessage(), $exception->getCode());
    }
    return $required;
  }

  /**
   * @param $data
   * @param $required
   * @return void
   * @throws \Exception
   */
  private function checkedData(&$data, $required)
  {
    $fields = [];
    foreach ($data as $key => $value) {
      if ($pos = strpos($key, '[')) {
        $field = substr($key, 0, $pos);
        $data[$field] = $value;
        unset($data[$key]);
        $fields[$field] = $key;
      }
    }

    $data = array_filter((new $this->entityClass($data))->jsonSerialize(), function ($value, $key) use ($required) {
      if (in_array($key, $required) && $value == '') {
        throw new \Exception($key . ' - 不能为空');
      }
      return !is_null($value);
    }, ARRAY_FILTER_USE_BOTH);
    foreach ($fields as $key => $field) {
      if (isset($data[$key])) {
        $data[$field] = $data[$key];
        unset($data[$key]);
      }
    }
  }

  /**
   * @param $result
   * @return mixed
   * @throws \Exception
   */
  private function returnResult($result)
  {
    $error = $this->db->errorInfo;
    if (is_null($error)) return $result;
    //数据表不存在，或字段不存在，主键冲突,创建或更新表
    if (is_array($error) && in_array($error[1], [1146, 1054, 1062])) {
      $this->db->initTable($this->tableName, $this->entityClass);
    }

    throw new \Exception($error[2], $error[1]);
  }
}
