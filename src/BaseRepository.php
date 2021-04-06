<?php

namespace Wanphp\Libray\Mysql;

abstract class BaseRepository implements BaseInterface
{
  protected $db;
  protected $tableName;
  private $entityClass;
  private $errCode = null;
  private $errMessage = '';

  public function __construct(Database $database, $tableName, $entityClass)
  {
    $this->db = $database;
    $this->tableName = $tableName;
    $this->entityClass = $entityClass;
  }

  /**
   * {@inheritDoc}
   */
  public function insert(array $datas): int
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

    if (!isset($datas[0])) $datas = [$datas];
    foreach ($datas as &$data) {
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
        if (in_array($key, $required) && ($value == '' || is_null($value))) {
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

    try {
      $this->db->insert($this->tableName, $datas);
    } catch (\Exception $e) {
      $this->errCode = $e->getCode();
      $this->errMessage = $e->getMessage();
    }
    return $this->returnResult($this->db->id() ?? 0);
  }

  /**
   * {@inheritDoc}
   */
  public function update(array $data, array $where): int
  {
    $fields = [];
    foreach ($data as $key => $value) {
      if ($pos = strpos($key, '[')) {
        $field = substr($key, 0, $pos);
        $data[$field] = $value;
        $fields[$field] = $key;
        unset($data[$key]);
      }
    }
    $data = array_filter((new $this->entityClass($data))->jsonSerialize(), function ($value) {
      return !is_null($value);
    });
    foreach ($fields as $key => $field) {
      if (isset($data[$key])) {
        $data[$field] = $data[$key];
        unset($data[$key]);
      }
    }
    try {
      $counts = $this->db->update($this->tableName, $data, $where)->rowCount();
    } catch (\Exception $e) {
      $this->errCode = $e->getCode();
      $this->errMessage = $e->getMessage();
    }
    return $this->returnResult($counts ?? 0);
  }

  /**
   * {@inheritDoc}
   */
  public function select(string $columns = '*', array $where = null): array
  {
    if ($columns != '*') $columns = explode(',', $columns);
    try {
      $datas = $this->db->select($this->tableName, $columns, $where);
    } catch (\Exception $e) {
      $this->errCode = $e->getCode();
      $this->errMessage = $e->getMessage();
    }
    return $this->returnResult($datas ?? []);
  }

  /**
   * {@inheritDoc}
   */
  public function get(string $columns = '*', array $where = null)
  {
    if ($columns != '*' && strpos($columns, ',') > 0) $columns = explode(',', $columns);
    try {
      $data = $this->db->get($this->tableName, $columns, $where);
    } catch (\Exception $e) {
      $this->errCode = $e->getCode();
      $this->errMessage = $e->getMessage();
    }
    return $this->returnResult($data ?? []);
  }

  /**
   * {@inheritDoc}
   */
  public function count(string $columns = '*', array $where = null): int
  {
    try {
      $count = $this->db->count($this->tableName, $columns, $where);
    } catch (\Exception $e) {
      $this->errCode = $e->getCode();
      $this->errMessage = $e->getMessage();
    }
    return $count ?? 0;
  }

  /**
   * {@inheritDoc}
   */
  public function sum(string $column, array $where = null): float
  {
    try {
      $total = $this->db->sum($this->tableName, $column, $where);
    } catch (\Exception $e) {
      $this->errCode = $e->getCode();
      $this->errMessage = $e->getMessage();
    }
    return $total ?? 0.00;
  }

  /**
   * {@inheritDoc}
   */
  public function delete(array $where): int
  {
    try {
      $counts = $this->db->delete($this->tableName, $where)->rowCount();
    } catch (\Exception $e) {
      $this->errCode = $e->getCode();
      $this->errMessage = $e->getMessage();
    }
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
   * @param $result
   * @return mixed
   * @throws \Exception
   */
  private function returnResult($result)
  {
    $error = $this->db->error(); //PHP8 取不到
    if (in_array($error[1], [1146, 1054, 1062])) {//数据表不存在，或字段不存在，主键冲突,创建或更新表
      $this->db->initTable($this->tableName, $this->entityClass);
    } elseif ($this->errCode) {
      if (in_array($this->errCode, ['42S02', '42S22'])) {
        $this->db->initTable($this->tableName, $this->entityClass);
      }
      throw new \Exception($this->errCode . ':' . $this->errMessage, $this->errCode);
    }

    if (is_null($error[1])) return $result;
    else throw new \Exception($error[1] . ' - ' . $this->tableName . ' ' . $error[2], $error[1]);
  }
}
