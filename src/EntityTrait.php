<?php

namespace Wanphp\Libray\Mysql;

trait EntityTrait
{
  /**
   * 初始化实体
   * @param array $array
   */
  public function __construct(array $array)
  {
    foreach (array_intersect_key($array, $this->jsonSerialize()) as $key => $value) {
      $this->{$key} = $value;
    }
  }

  /**
   * @param $name
   * @param null $arguments
   * @return mixed|null
   */
  public function __call($name, $arguments = null)
  {
    $action = substr($name, 0, 3);
    $property = strtolower(substr($name, 3));
    if ($action == 'set' && property_exists($this, $property)) {
      $this->{$property} = $arguments;
      return $arguments;
    } elseif ($action == 'get' && property_exists($this, $property)) {
      return $this->{$property};
    } else {
      return null;
    }
  }
}
