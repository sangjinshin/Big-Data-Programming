package com.sangjinshin.cs378.assign11

/**
 * Event Class
 * 
 * @author Sangjin Shin
 * UTEID: ss62273
 * Email: sangjinshin@outlook.com
 */
protected class Event(val eventType: String, val eventSubtype: String, val eventTimestamp: String, val vin: String) extends Serializable {
  private var _eventType: String = eventType
  private var _eventSubtype: String = eventSubtype
  private var _eventTimestamp: String = eventTimestamp
  private var _vin: String = vin

  // A secondary constructor with empty parameters
  def this() {
    this("", "", "", "")
  }
  
  def getEventType: String = _eventType
  def setEventType(eventType: String): Unit = {
    _eventType = eventType
  }
  
  def getEventSubtype: String = _eventSubtype
  def setEventSubtype(eventSubtype: String): Unit = {
    _eventSubtype = eventSubtype
  }
  
  def getEventTimestamp: String = _eventTimestamp
  def setEventTimestamp(eventTimestamp: String): Unit = {
    _eventTimestamp = eventTimestamp
  }
  
  def getVin: String = _vin
  def setVin(vin: String): Unit = {
    _vin = vin
  }

  override def toString(): String = {
    ("<" + _eventType + ":" + _eventSubtype + "," + _eventTimestamp + "," + _vin + ">")
  }
}