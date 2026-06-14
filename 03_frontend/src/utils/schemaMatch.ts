import type { DiscoveredAsset } from '../types/agents';

function getFieldNames(asset: DiscoveredAsset): Set<string> {
  const meta = asset.metadata ?? {};
  const raw = (meta.fields as Array<{ name: string }>) ??
              (meta.schema as { fields?: Array<{ name: string }> })?.fields ??
              [];

  return new Set(
    raw.map(f => f.name.toLowerCase().replace(/[_\-]/g, ''))
  );
}

export function computeSchemaMatch(topic: DiscoveredAsset, table: DiscoveredAsset): number {
  const topicFields = getFieldNames(topic);
  const tableFields = getFieldNames(table);

  if (topicFields.size === 0 || tableFields.size === 0) return 0;

  let intersection = 0;
  topicFields.forEach(f => {
    if (tableFields.has(f)) intersection++;
  });

  return intersection / Math.max(topicFields.size, tableFields.size);
}

export function findRelatedTables(
  asset: DiscoveredAsset,
  allAssets: DiscoveredAsset[],
  threshold: number = 0.5,
  maxResults: number = 3
): Array<{ asset: DiscoveredAsset; score: number; matchedFields: string[] }> {
  // Show Iceberg tables when viewing Kafka topic
  if (asset.asset_type === 'kafka_topic') {
    const topicFields = getFieldNames(asset);
    if (topicFields.size === 0) return [];

    const matches = allAssets
      .filter(a => a.asset_type === 'iceberg_table')
      .map(table => {
        const score = computeSchemaMatch(asset, table);
        if (score < threshold) return null;

        const tableFields = getFieldNames(table);
        const matchedFieldNames: string[] = [];

        topicFields.forEach(f => {
          if (tableFields.has(f)) {
            const meta = asset.metadata ?? {};
            const originalName = ((meta.schema as any)?.fields as Array<{ name: string }> | undefined)?.find(
              field => field.name.toLowerCase().replace(/[_\-]/g, '') === f
            )?.name ??
            ((meta.fields as Array<{ name: string }> | undefined)?.find(
              field => field.name.toLowerCase().replace(/[_\-]/g, '') === f
            )?.name) ??
            f;
            matchedFieldNames.push(originalName);
          }
        });

        return { asset: table, score, matchedFields: matchedFieldNames };
      })
      .filter((m): m is { asset: DiscoveredAsset; score: number; matchedFields: string[] } => m !== null)
      .sort((a, b) => b.score - a.score)
      .slice(0, maxResults);

    return matches;
  }

  // Show Kafka topics when viewing Iceberg table
  if (asset.asset_type === 'iceberg_table') {
    const tableFields = getFieldNames(asset);
    if (tableFields.size === 0) return [];

    const matches = allAssets
      .filter(a => a.asset_type === 'kafka_topic')
      .map(topic => {
        const score = computeSchemaMatch(topic, asset);
        if (score < threshold) return null;

        const topicFields = getFieldNames(topic);
        const matchedFieldNames: string[] = [];

        tableFields.forEach(f => {
          if (topicFields.has(f)) {
            const meta = asset.metadata ?? {};
            const originalName = ((meta.fields as any)?.fields as Array<{ name: string }> | undefined)?.find(
              field => field.name.toLowerCase().replace(/[_\-]/g, '') === f
            )?.name ??
            ((meta.fields as Array<{ name: string }> | undefined)?.find(
              field => field.name.toLowerCase().replace(/[_\-]/g, '') === f
            )?.name) ??
            f;
            matchedFieldNames.push(originalName);
          }
        });

        return { asset: topic, score, matchedFields: matchedFieldNames };
      })
      .filter((m): m is { asset: DiscoveredAsset; score: number; matchedFields: string[] } => m !== null)
      .sort((a, b) => b.score - a.score)
      .slice(0, maxResults);

    return matches;
  }

  return [];
}
