package com.hotels.bdp.waggledance.mapping.service.requests;

import java.util.concurrent.Callable;

import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;

public interface RequestCallable<T> extends Callable<T> {

  DatabaseMapping getMapping();

}
