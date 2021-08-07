package com.mt.mall.application.catalog;

import com.github.fge.jsonpatch.JsonPatch;
import com.mt.common.domain.CommonDomainRegistry;
import com.mt.common.domain.model.domain_event.SubscribeForEvent;
import com.mt.common.domain.model.restful.SumPagedRep;
import com.mt.common.domain.model.restful.query.QueryUtility;
import com.mt.mall.application.ApplicationServiceRegistry;
import com.mt.mall.application.catalog.command.CreateCatalogCommand;
import com.mt.mall.application.catalog.command.PatchCatalogCommand;
import com.mt.mall.application.catalog.command.UpdateCatalogCommand;
import com.mt.mall.application.catalog.representation.CatalogCardRepresentation;
import com.mt.mall.domain.DomainRegistry;
import com.mt.mall.domain.model.catalog.Catalog;
import com.mt.mall.domain.model.catalog.CatalogId;
import com.mt.mall.domain.model.catalog.CatalogQuery;
import com.mt.mall.domain.model.catalog.LinkedTag;
import com.mt.mall.domain.model.meta.Meta;
import com.mt.mall.domain.model.meta.MetaQuery;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class CatalogApplicationService {
    @SubscribeForEvent
    @Transactional
    public String create(CreateCatalogCommand command, String changeId) {
        return ApplicationServiceRegistry.getIdempotentWrapper().idempotent(changeId,
                (change) -> {
                    CatalogId catalogId = new CatalogId();
                    DomainRegistry.getCatalogService().create(
                            catalogId,
                            command.getName(),
                            command.getParentId() != null && !command.getParentId().isBlank() ? new CatalogId(command.getParentId()) : null,
                            command.getAttributes().stream().map(LinkedTag::new).collect(Collectors.toSet()),
                            command.getCatalogType()
                    );
                    return catalogId.getDomainId();
                }, "Catalog"
        );
    }

    public SumPagedRep<CatalogCardRepresentation> catalogs(String queryParam, String pageParam, String skipCount) {
        SumPagedRep<Catalog> catalogs = DomainRegistry.getCatalogRepository().catalogsOfQuery(new CatalogQuery(queryParam, pageParam, skipCount));
        Set<CatalogId> collect = catalogs.getData().stream().map(Catalog::getCatalogId).collect(Collectors.toSet());
        Set<Meta> allByQuery = QueryUtility.getAllByQuery(e -> DomainRegistry.getMetaRepository().metaOfQuery((MetaQuery) e), new MetaQuery(new HashSet<>(collect)));
        List<CatalogCardRepresentation> collect1 = catalogs.getData().stream().map(e -> {
            boolean review = false;
            Optional<Meta> first = allByQuery.stream().filter(ee -> ee.getDomainId().getDomainId().equalsIgnoreCase(e.getCatalogId().getDomainId())).findFirst();
            if (first.isPresent() && first.get().getHasChangedTag()) {
                review = true;
            }
            return new CatalogCardRepresentation(e, review);
        }).collect(Collectors.toList());
        return new SumPagedRep<>(collect1, catalogs.getTotalItemCount());
    }

    public SumPagedRep<Catalog> publicCatalogs(String pageParam, String skipCount) {
        return DomainRegistry.getCatalogRepository().catalogsOfQuery(CatalogQuery.publicQuery(pageParam, skipCount));
    }

    public Optional<Catalog> catalog(String id) {
        return DomainRegistry.getCatalogRepository().catalogOfId(new CatalogId(id));
    }

    @SubscribeForEvent
    @Transactional
    public void replace(String id, UpdateCatalogCommand command, String changeId) {
        ApplicationServiceRegistry.getIdempotentWrapper().idempotent(changeId, (change) -> {
            CatalogId catalogId = new CatalogId(id);
            Optional<Catalog> optionalCatalog = DomainRegistry.getCatalogRepository().catalogOfId(catalogId);
            if (optionalCatalog.isPresent()) {
                Catalog catalog = optionalCatalog.get();
                catalog.replace(command.getName(), new CatalogId(command.getParentId()), command.getAttributes().stream().map(LinkedTag::new).collect(Collectors.toSet()), command.getCatalogType());
                DomainRegistry.getCatalogRepository().add(catalog);
            }
            return null;
        }, "Catalog");
    }

    @SubscribeForEvent
    @Transactional
    public void removeCatalog(String id, String changeId) {
        ApplicationServiceRegistry.getIdempotentWrapper().idempotent(changeId, (change) -> {
            CatalogId catalogId = new CatalogId(id);
            Optional<Catalog> optionalCatalog = DomainRegistry.getCatalogRepository().catalogOfId(catalogId);
            if (optionalCatalog.isPresent()) {
                Catalog catalog = optionalCatalog.get();
                DomainRegistry.getCatalogRepository().remove(catalog);
            }
            return null;
        }, "Catalog");
    }

    @SubscribeForEvent
    @Transactional
    public void patch(String id, JsonPatch command, String changeId) {
        ApplicationServiceRegistry.getIdempotentWrapper().idempotent(changeId, (change) -> {
            CatalogId catalogId = new CatalogId(id);
            Optional<Catalog> optionalCatalog = DomainRegistry.getCatalogRepository().catalogOfId(catalogId);
            if (optionalCatalog.isPresent()) {
                Catalog catalog = optionalCatalog.get();
                PatchCatalogCommand beforePatch = new PatchCatalogCommand(catalog);
                PatchCatalogCommand afterPatch = CommonDomainRegistry.getCustomObjectSerializer().applyJsonPatch(command, beforePatch, PatchCatalogCommand.class);
                catalog.replace(
                        afterPatch.getName(),
                        new CatalogId(afterPatch.getParentId()),
                        afterPatch.getAttributes().stream().map(LinkedTag::new).collect(Collectors.toSet()),
                        afterPatch.getType()
                );
            }
            return null;
        }, "Catalog");
    }
}
