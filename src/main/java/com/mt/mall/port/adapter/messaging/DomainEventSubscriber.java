package com.mt.mall.port.adapter.messaging;

import com.mt.common.domain.CommonDomainRegistry;
import com.mt.common.domain.model.restful.exception.AggregateOutdatedException;
import com.mt.common.domain.model.sql.builder.UpdateQueryBuilder;
import com.mt.mall.application.ApplicationServiceRegistry;
import com.mt.mall.application.sku.command.InternalSkuPatchCommand;
import com.mt.mall.infrastructure.AppConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import static com.mt.mall.domain.model.catalog.event.CatalogEvent.TOPIC_CATALOG;
import static com.mt.mall.domain.model.filter.event.FilterEvent.TOPIC_FILTER;
import static com.mt.mall.domain.model.product.event.ProductCreated.TOPIC_PRODUCT;
import static com.mt.mall.domain.model.tag.event.TagCriticalFieldChanged.TOPIC_TAG;

@Slf4j
@Component
public class DomainEventSubscriber {
    private static final String SKU_QUEUE_NAME = "sku_queue"+AppConstant.APP_HANDLER;
    private static final String META_QUEUE_NAME = "meta_queue"+AppConstant.APP_HANDLER;
    private static final String SKU_EX_QUEUE_NAME = AppConstant.DECREASE_ORDER_STORAGE_FOR_CREATE_EVENT +AppConstant.APP_HANDLER;
    private static final String SKU_EX_QUEUE_NAME2 = AppConstant.CANCEL_DECREASE_ORDER_STORAGE_FOR_CREATE_EVENT +AppConstant.APP_HANDLER;
    private static final String SKU_EX_QUEUE_NAME3 = AppConstant.DECREASE_ACTUAL_STORAGE_FOR_CONCLUDE_EVENT +AppConstant.APP_HANDLER;
    private static final String SKU_EX_QUEUE_NAME4 = AppConstant.INCREASE_ORDER_STORAGE_FOR_RECYCLE_EVENT +AppConstant.APP_HANDLER;
    private static final String SKU_EX_QUEUE_NAME5 = AppConstant.DECREASE_ORDER_STORAGE_FOR_RESERVE_EVENT +AppConstant.APP_HANDLER;
    private static final String SKU_EX_QUEUE_NAME6 = AppConstant.CANCEL_DECREASE_ACTUAL_STORAGE_FOR_CONCLUDE_EVENT +AppConstant.APP_HANDLER;
    private static final String SKU_EX_QUEUE_NAME7 = AppConstant.CANCEL_INCREASE_ORDER_STORAGE_FOR_RECYCLE_EVENT +AppConstant.APP_HANDLER;
    private static final String SKU_EX_QUEUE_NAME8 = AppConstant.CANCEL_DECREASE_ORDER_STORAGE_FOR_RESERVE_EVENT +AppConstant.APP_HANDLER;
    @Value("${spring.application.name}")
    private String appName;
    @Value("${mt.app.name.mt15}")
    private String sagaName;

    @EventListener(ApplicationReadyEvent.class)
    private void skuListener() {
        CommonDomainRegistry.getEventStreamService().subscribe(appName, true, SKU_QUEUE_NAME, (event) -> {
            try {
                ApplicationServiceRegistry.getSkuApplicationService().handleChange(event);
            } catch (UpdateQueryBuilder.PatchCommandExpectNotMatchException | AggregateOutdatedException ex) {
                //ignore above ex
                log.debug("ignore exception in event {}", ex.getClass().toString());
            }
        }, TOPIC_PRODUCT);
    }

    @EventListener(ApplicationReadyEvent.class)
    private void metaChangeListener() {
        CommonDomainRegistry.getEventStreamService().subscribe(appName, true, META_QUEUE_NAME, (event) -> {
            ApplicationServiceRegistry.getMetaApplicationService().handleChange(event);
        }, TOPIC_TAG, TOPIC_PRODUCT, TOPIC_CATALOG, TOPIC_FILTER);
    }

    @EventListener(ApplicationReadyEvent.class)
    private void skuExternalListener() {
        CommonDomainRegistry.getEventStreamService().subscribe(sagaName, false, SKU_EX_QUEUE_NAME, (event) -> {
            log.debug("handling {} with id {}",AppConstant.DECREASE_ORDER_STORAGE_FOR_CREATE_REPLY_EVENT, event.getId());
            InternalSkuPatchCommand deserialize = CommonDomainRegistry.getCustomObjectSerializer().deserialize(event.getEventBody(), InternalSkuPatchCommand.class);
            ApplicationServiceRegistry.getSkuApplicationService().handle(deserialize,
                    AppConstant.DECREASE_ORDER_STORAGE_FOR_CREATE_REPLY_EVENT);
        }, AppConstant.DECREASE_ORDER_STORAGE_FOR_CREATE_EVENT);
    }

    @EventListener(ApplicationReadyEvent.class)
    private void skuExternalListener2() {
        CommonDomainRegistry.getEventStreamService().subscribe(sagaName, false, SKU_EX_QUEUE_NAME2, (event) -> {
            log.debug("handling {} with id {}",AppConstant.CANCEL_DECREASE_ORDER_STORAGE_FOR_CREATE_EVENT, event.getId());
            InternalSkuPatchCommand deserialize = CommonDomainRegistry.getCustomObjectSerializer().deserialize(event.getEventBody(), InternalSkuPatchCommand.class);
            ApplicationServiceRegistry.getSkuApplicationService().handleCancel(deserialize,
                    AppConstant.CANCEL_DECREASE_ORDER_STORAGE_FOR_CREATE_REPLY_EVENT);
        }, AppConstant.CANCEL_DECREASE_ORDER_STORAGE_FOR_CREATE_EVENT);
    }

    @EventListener(ApplicationReadyEvent.class)
    private void skuExternalListener3() {
        CommonDomainRegistry.getEventStreamService().subscribe(sagaName, false, SKU_EX_QUEUE_NAME3, (event) -> {
            log.debug("handling {} with id {}",AppConstant.DECREASE_ACTUAL_STORAGE_FOR_CONCLUDE_EVENT, event.getId());
            InternalSkuPatchCommand deserialize = CommonDomainRegistry.getCustomObjectSerializer().deserialize(event.getEventBody(), InternalSkuPatchCommand.class);
            ApplicationServiceRegistry.getSkuApplicationService().handle(deserialize,
                    AppConstant.DECREASE_ACTUAL_STORAGE_FOR_CONCLUDE_REPLY_EVENT);
        }, AppConstant.DECREASE_ACTUAL_STORAGE_FOR_CONCLUDE_EVENT);
    }
    @EventListener(ApplicationReadyEvent.class)
    private void skuExternalListener4() {
        CommonDomainRegistry.getEventStreamService().subscribe(sagaName, false, SKU_EX_QUEUE_NAME4, (event) -> {
            log.debug("handling {} with id {}",AppConstant.INCREASE_ORDER_STORAGE_FOR_RECYCLE_EVENT, event.getId());
            InternalSkuPatchCommand deserialize = CommonDomainRegistry.getCustomObjectSerializer().deserialize(event.getEventBody(), InternalSkuPatchCommand.class);
            ApplicationServiceRegistry.getSkuApplicationService().handle(deserialize,
                    AppConstant.INCREASE_ORDER_STORAGE_FOR_RECYCLE_REPLY_EVENT);
        }, AppConstant.INCREASE_ORDER_STORAGE_FOR_RECYCLE_EVENT);
    }
    @EventListener(ApplicationReadyEvent.class)
    private void skuExternalListener5() {
        CommonDomainRegistry.getEventStreamService().subscribe(sagaName, false, SKU_EX_QUEUE_NAME5, (event) -> {
            log.debug("handling {} with id {}",AppConstant.DECREASE_ORDER_STORAGE_FOR_RESERVE_EVENT, event.getId());
            InternalSkuPatchCommand deserialize = CommonDomainRegistry.getCustomObjectSerializer().deserialize(event.getEventBody(), InternalSkuPatchCommand.class);
            ApplicationServiceRegistry.getSkuApplicationService().handle(deserialize,
                    AppConstant.DECREASE_ORDER_STORAGE_FOR_RESERVE_REPLY_EVENT);
        }, AppConstant.DECREASE_ORDER_STORAGE_FOR_RESERVE_EVENT);
    }
    @EventListener(ApplicationReadyEvent.class)
    private void skuExternalListener6() {
        CommonDomainRegistry.getEventStreamService().subscribe(sagaName, false, SKU_EX_QUEUE_NAME6, (event) -> {
            log.debug("handling {} with id {}",AppConstant.CANCEL_DECREASE_ACTUAL_STORAGE_FOR_CONCLUDE_EVENT, event.getId());
            InternalSkuPatchCommand deserialize = CommonDomainRegistry.getCustomObjectSerializer().deserialize(event.getEventBody(), InternalSkuPatchCommand.class);
            ApplicationServiceRegistry.getSkuApplicationService().handleCancel(deserialize,
                    AppConstant.CANCEL_DECREASE_ACTUAL_STORAGE_FOR_CONCLUDE_REPLY_EVENT);
        }, AppConstant.CANCEL_DECREASE_ACTUAL_STORAGE_FOR_CONCLUDE_EVENT);
    }
    @EventListener(ApplicationReadyEvent.class)
    private void skuExternalListener7() {
        CommonDomainRegistry.getEventStreamService().subscribe(sagaName, false, SKU_EX_QUEUE_NAME7, (event) -> {
            log.debug("handling {} with id {}",AppConstant.CANCEL_INCREASE_ORDER_STORAGE_FOR_RECYCLE_EVENT, event.getId());
            InternalSkuPatchCommand deserialize = CommonDomainRegistry.getCustomObjectSerializer().deserialize(event.getEventBody(), InternalSkuPatchCommand.class);
            ApplicationServiceRegistry.getSkuApplicationService().handleCancel(deserialize,
                    AppConstant.CANCEL_INCREASE_ORDER_STORAGE_FOR_RECYCLE_REPLY_EVENT);
        }, AppConstant.CANCEL_INCREASE_ORDER_STORAGE_FOR_RECYCLE_EVENT);
    }
    @EventListener(ApplicationReadyEvent.class)
    private void skuExternalListener8() {
        CommonDomainRegistry.getEventStreamService().subscribe(sagaName, false, SKU_EX_QUEUE_NAME8, (event) -> {
            log.debug("handling {} with id {}",AppConstant.CANCEL_DECREASE_ORDER_STORAGE_FOR_RESERVE_EVENT, event.getId());
            InternalSkuPatchCommand deserialize = CommonDomainRegistry.getCustomObjectSerializer().deserialize(event.getEventBody(), InternalSkuPatchCommand.class);
            ApplicationServiceRegistry.getSkuApplicationService().handleCancel(deserialize,
                    AppConstant.CANCEL_DECREASE_ORDER_STORAGE_FOR_RESERVE_REPLY_EVENT);
        }, AppConstant.CANCEL_DECREASE_ORDER_STORAGE_FOR_RESERVE_EVENT);
    }
}
