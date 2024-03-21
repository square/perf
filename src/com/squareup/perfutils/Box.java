/*
 * Copyright 2024 Block Inc.
 */

package com.squareup.perfutils;


/**
 * Instances of this class are used for holding and persisting references to other objects. This
 * class works around Java lambda's lack of capture by reference semantics for local variables by
 * allowing the reference to be passed explicitly inside an object. The contained object can only
 * be garbage-collected after all lambdas holding references to this object have exited.
 *
 * Note that the methods of this class do not implement thread synchronization, so is not
 * thread-safe.
 *
 * This class is used instead of `AtomicReference` because more than 50% of the latter's functions
 * are not relevant to the purposes that Box was built for, and the unused thread-related semantics are
 * likely to confuse people reading the code that uses it.
 */
public class Box<T> {
  /**
   * The object reference being persisted.
   */
  private T value;

  /**
   * Constructor with initial value.
   *
   * @param t The initial reference that this Box will hold.
   */
  public Box(T t) {
    value = t;
  }

  /**
   * Constructor with no initial value.
   */
  public Box() {
    this(null);
  }

  /**
   * Obtain the most recently stored reference.
   *
   * @return The most recently stored reference, or null if nothing is stored.
   */
  public T get() {
    return value;
  }

  /**
   * Store a reference into this object.
   *
   * @param t A new reference to insert into the Box.
   */
  public void set(T t) {
    value = t;
  }
}
