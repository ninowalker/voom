======================================
 Voom - Event Bus & Messaging Gateway
======================================

.. currentmodule:: voom

:Author: Nino Walker
:Version: |release|
:Date: 2032/03/19
:Homepage: `Voom on Github <https://github.com/ninowalker/voom>`_
:Download: `Voom on Github`_
:License: `BSD License`_
:Issue tracker: `Github Issues
 <https://github.com/ninowalker/voom/issues>`_
:Wiki: `Github Wiki
 <https://github.com/ninowalker/voom/wiki>`_

.. _BSD License: https://github.com/ninowalker/voom/blob/master/LICENSE

.. currentmodule:: voom

.. module:: voom
   :synopsis: A library for event-based processing and messaging in python.

.. index:: introduction

Hello


API Documentation
=================

.. toctree::
   :maxdepth: 2

   voom_bus
   voom_amqp

Quick Guide
===========

:class:`VoomBus` provides a simple interface

.. doctest::

   >>> import sys
   >>> from voom.bus import VoomBus
   >>> from voom.priorities import BusPriority
   >>> bus = VoomBus()
   >>> bus.subscribe(int, lambda msg: sys.stdout.write("got it %d\n" % msg))
   >>> bus.subscribe(int, lambda msg: sys.stdout.write("squared %d\n" % msg**2), priority=BusPriority.LOW_PRIORITY)
   >>> bus.publish("hello, I'm a string.")
   >>> bus.publish(101)
   got it 101
   squared 10201
   
   >>>



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

