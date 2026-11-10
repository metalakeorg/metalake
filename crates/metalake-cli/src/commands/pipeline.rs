//! `metalake pipeline`: a continuous pipeline's verbs, each the client's call, answered as the rows `SHOW PIPELINES`
//! gives.

use metalake_client::{Error, Lake, Pipeline, PipelineOptions, Prepared, pipeline_rows};

use crate::args::PipelineVerb;
use crate::out::Out;

pub async fn run(verb: &PipelineVerb, lake: &Lake, out: &Out) -> Result<(), Error> {
    let pipelines: Vec<Pipeline> = match verb {
        PipelineVerb::Create(create) => {
            let query = match (&create.query, &create.file) {
                (Some(query), _) => query.clone(),
                (None, Some(file)) => {
                    std::fs::read_to_string(file).map_err(|e| Error::failed(format!("{}: {e}", file.display())))?
                }
                (None, None) => return Err(Error::usage("a pipeline's query: --query, or --file")),
            };
            let options = PipelineOptions {
                parallelism: create.parallelism,
                empty_target: create.empty_target,
            };
            vec![lake.create_pipeline(&create.name, &query, options).await?]
        }
        PipelineVerb::List => lake.pipelines().await?,
        PipelineVerb::Describe(p) => vec![lake.pipeline(&p.name).await?],
        PipelineVerb::Suspend(p) => vec![lake.suspend_pipeline(&p.name).await?],
        PipelineVerb::Resume(p) => vec![lake.resume_pipeline(&p.name).await?],
        PipelineVerb::Drop(p) => {
            lake.drop_pipeline(&p.name).await?;
            return Ok(());
        }
    };
    let (schema, batch) = pipeline_rows(&pipelines)?;
    out.rows(Prepared::from_batches(schema, vec![batch])?.read()?).await
}
