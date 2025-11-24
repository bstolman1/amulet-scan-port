import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.39.3';

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

interface TemplateFile {
  filename: string;
  content: string;
  templateId?: string;
  chunkIndex?: number;
  totalChunks?: number;
  isChunked?: boolean;
}

interface StartRequest {
  mode: 'start';
  summary: {
    sv_url: string;
    migration_id: number;
    record_time: string;
    canonical_package: string;
    totals: {
      amulet: string;
      locked: string;
      circulating: string;
    };
    entry_count: number;
  };
  webhookSecret: string;
}

interface AppendRequest {
  mode: 'append';
  snapshot_id: string;
  templates: TemplateFile[];
  webhookSecret: string;
}

interface CompleteRequest {
  mode: 'complete';
  snapshot_id: string;
  webhookSecret: string;
  summary?: {
    totals: {
      amulet: string;
      locked: string;
      circulating: string;
    };
    entry_count: number;
    canonical_package: string;
  };
}

interface ProgressRequest {
  mode: 'progress';
  snapshot_id: string;
  webhookSecret: string;
  progress: {
    processed_pages: number;
    processed_events: number;
    elapsed_time_ms: number;
    pages_per_minute: number;
  };
}

type UploadRequest = StartRequest | AppendRequest | CompleteRequest | ProgressRequest;

/**
 * Process a single template chunk with memory optimization and error handling
 */
async function processTemplateChunk(
  supabase: any,
  snapshot_id: string,
  template: TemplateFile
): Promise<number> {
  const isChunked = template.isChunked || false;
  const templateId = template.templateId || template.filename.replace(/\.json$/, '').replace(/_/g, ':');
  const chunkIndex = template.chunkIndex || 0;
  const totalChunks = template.totalChunks || 1;

  // Determine storage path based on chunking
  let storagePath: string;
  if (isChunked) {
    storagePath = `${snapshot_id}/chunks/${template.filename}`;
  } else {
    storagePath = `${snapshot_id}/templates/${template.filename}`;
  }

  // Upload the file to storage
  const fileContent = new TextEncoder().encode(template.content);
  const { error: uploadError } = await supabase.storage
    .from('acs-data')
    .upload(storagePath, fileContent, {
      contentType: 'application/json',
      upsert: true,
    });

  if (uploadError) {
    console.error(`Failed to upload ${template.filename}:`, uploadError);
    throw uploadError;
  }

  // Parse JSON only to count contracts, then immediately release
  let contractCount: number;
  {
    const data = JSON.parse(template.content);
    contractCount = data.length;
    // data goes out of scope here and can be garbage collected
  }

  // Handle chunked vs non-chunked storage
  if (isChunked) {
    // For chunked uploads, accumulate stats
    const { data: existingStats } = await supabase
      .from('acs_template_stats')
      .select('contract_count')
      .eq('snapshot_id', snapshot_id)
      .eq('template_id', templateId)
      .maybeSingle();

    const newContractCount = (existingStats?.contract_count || 0) + contractCount;

    // Update manifest with chunk info
    const manifestPath = `${snapshot_id}/manifests/${templateId.replace(/:/g, '_')}_manifest.json`;
    const { data: existingManifestFile } = await supabase.storage
      .from('acs-data')
      .download(manifestPath)
      .catch(() => ({ data: null }));

    interface ChunkManifest {
      chunks: Array<{ chunkIndex: number; contractCount: number; storagePath: string }>;
      totalEntries: number;
      totalChunks: number;
    }

    let manifest: ChunkManifest = { chunks: [], totalEntries: 0, totalChunks };
    if (existingManifestFile) {
      const text = await existingManifestFile.text();
      manifest = JSON.parse(text);
    }

    manifest.chunks.push({ chunkIndex, contractCount, storagePath });
    manifest.totalEntries += contractCount;

    const manifestContent = new TextEncoder().encode(JSON.stringify(manifest, null, 2));
    await supabase.storage
      .from('acs-data')
      .upload(manifestPath, manifestContent, {
        contentType: 'application/json',
        upsert: true
      });

    // Update stats to point to manifest
    await supabase
      .from('acs_template_stats')
      .upsert({
        snapshot_id,
        template_id: templateId,
        template_name: templateId,
        round: 0,
        instance_count: 0,
        contract_count: newContractCount,
        storage_path: manifestPath,
      }, {
        onConflict: 'snapshot_id,template_id'
      });

    console.log(`  Chunk ${chunkIndex + 1}/${totalChunks}: ${contractCount} contracts (total: ${newContractCount})`);
  } else {
    // For non-chunked uploads, simple insert/update
    await supabase
      .from('acs_template_stats')
      .upsert({
        snapshot_id,
        template_id: templateId,
        template_name: templateId,
        round: 0,
        instance_count: 0,
        contract_count: contractCount,
        storage_path: storagePath,
      }, {
        onConflict: 'snapshot_id,template_id'
      });
  }

  return contractCount;
}

Deno.serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const request: UploadRequest = await req.json();

    // Verify webhook secret
    const expectedSecret = Deno.env.get('ACS_UPLOAD_WEBHOOK_SECRET');
    if (!expectedSecret || request.webhookSecret !== expectedSecret) {
      console.error('Invalid webhook secret');
      return new Response(
        JSON.stringify({ error: 'Unauthorized' }),
        { status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Initialize Supabase client with service role key
    const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
    const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
    const supabase = createClient(supabaseUrl, supabaseServiceKey);

    // Handle different modes
    if (request.mode === 'start') {
      console.log('Creating snapshot record...');

      const { data: snapshot, error: snapshotError } = await supabase
        .from('acs_snapshots')
        .insert({
          round: request.summary.migration_id,
          snapshot_data: {},
          sv_url: request.summary.sv_url,
          migration_id: request.summary.migration_id,
          record_time: request.summary.record_time,
          canonical_package: request.summary.canonical_package,
          amulet_total: request.summary.totals.amulet,
          locked_total: request.summary.totals.locked,
          circulating_supply: request.summary.totals.circulating,
          entry_count: request.summary.entry_count,
          status: 'processing',
        })
        .select()
        .single();

      if (snapshotError) {
        console.error('Snapshot creation error:', snapshotError);
        throw snapshotError;
      }

      console.log(`Created snapshot: ${snapshot.id}`);

      return new Response(
        JSON.stringify({
          success: true,
          snapshot_id: snapshot.id
        }),
        {
          status: 200,
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        }
      );
    }

    if (request.mode === 'append') {
      const { snapshot_id, templates } = request;
      console.log(`Processing batch of ${templates.length} templates for snapshot ${snapshot_id}`);

      // Check if snapshot is still active (not stale)
      const { data: snapshot, error: snapshotCheckError } = await supabase
        .from('acs_snapshots')
        .select('status, updated_at')
        .eq('id', snapshot_id)
        .single();

      if (snapshotCheckError) {
        console.error('Failed to check snapshot status:', snapshotCheckError);
        return new Response(
          JSON.stringify({ error: 'Snapshot not found' }),
          { status: 404, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      // Reject if snapshot is failed or completed
      if (snapshot.status === 'failed' || snapshot.status === 'completed') {
        console.warn(`Rejecting append to ${snapshot.status} snapshot ${snapshot_id}`);
        return new Response(
          JSON.stringify({ error: `Snapshot is ${snapshot.status}`, snapshot_id }),
          { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      let processed = 0;
      let totalContractsAdded = 0;
      const errors = [];

      // Process templates sequentially with memory optimization
      for (let i = 0; i < templates.length; i++) {
        const template = templates[i];
        console.log(`Processing template ${i + 1}/${templates.length}: ${template.filename}`);

        try {
          // Process this template in isolation to allow garbage collection
          const contractCount = await processTemplateChunk(
            supabase,
            snapshot_id,
            template
          );

          totalContractsAdded += contractCount;
          processed++;

          console.log(`Completed ${template.filename}: ${contractCount} contracts`);
        } catch (error) {
          console.error(`Failed to process ${template.filename}:`, error);
          errors.push({
            filename: template.filename,
            error: error instanceof Error ? error.message : String(error)
          });
        }

        // Explicit cleanup - allow GC to collect template data
        templates[i] = null as any;
      }

      // If we had any errors, return 546 with details
      if (errors.length > 0) {
        console.error(`Batch completed with ${errors.length}/${templates.length} errors`);
        return new Response(
          JSON.stringify({
            error: 'Partial upload failure',
            processed,
            failed: errors.length,
            total: templates.length,
            errors: errors.slice(0, 5), // Return first 5 errors for debugging
          }),
          {
            status: 546,
            headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          }
        );
      }

      console.log(`Processed ${processed} templates, ${totalContractsAdded} contracts`);

      return new Response(
        JSON.stringify({
          success: true,
          processed: processed,
          contracts_added: totalContractsAdded
        }),
        {
          status: 200,
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        }
      );
    }

    if (request.mode === 'progress') {
      const { snapshot_id } = request;
      console.log(`Progress update received for snapshot ${snapshot_id}`);

      return new Response(
        JSON.stringify({
          success: true
        }),
        {
          status: 200,
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        }
      );
    }

    if (request.mode === 'complete') {
      const { snapshot_id, summary } = request;
      console.log(`Marking snapshot ${snapshot_id} as completed`);

      const updateData: any = {
        status: 'completed',
      };

      // Update with final totals if summary is provided
      if (summary) {
        updateData.amulet_total = summary.totals.amulet;
        updateData.locked_total = summary.totals.locked;
        updateData.circulating_supply = summary.totals.circulating;
        updateData.entry_count = summary.entry_count;
        updateData.canonical_package = summary.canonical_package;
      }

      const { error: updateError } = await supabase
        .from('acs_snapshots')
        .update(updateData)
        .eq('id', snapshot_id);

      if (updateError) {
        console.error('Failed to mark snapshot as completed:', updateError);
        throw updateError;
      }

      console.log('Snapshot marked as completed with final totals');

      return new Response(
        JSON.stringify({
          success: true
        }),
        {
          status: 200,
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        }
      );
    }

    return new Response(
      JSON.stringify({ error: 'Invalid mode' }),
      {
        status: 400,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      }
    );

  } catch (error) {
    console.error('Upload failed:', error);
    const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
    return new Response(
      JSON.stringify({ error: errorMessage }),
      {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      }
    );
  }
});
