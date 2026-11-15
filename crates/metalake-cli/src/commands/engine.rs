//! `metalake engine`: the engines named in the lake's file, answered as the rows `engine list` gives. Nothing is reached
//! but the file.

use std::path::Path;

use metalake_client::{EngineConfig, EngineKind, Engines, Error, LakeFile, Prepared, engine_rows};

use crate::args::EngineVerb;
use crate::out::Out;

pub async fn run(verb: &EngineVerb, dir: Option<&Path>, out: &Out) -> Result<(), Error> {
    let file = LakeFile::find(dir)?;
    let shown: Engines = match verb {
        EngineVerb::Add(add) => {
            let config = EngineConfig {
                kind: EngineKind::parse(&add.kind)?,
                url: add.url.clone(),
                user: add.user.clone(),
                token_env: add.token_env.clone(),
                password_env: add.password_env.clone(),
                catalog: add.catalog.clone(),
            };
            file.add_engine(&add.name, &config, add.replace)?;
            Engines::from([(add.name.clone(), config)])
        }
        EngineVerb::List => file.engines()?,
        EngineVerb::Remove(engine) => {
            file.remove_engine(&engine.name)?;
            return Ok(());
        }
    };
    let (schema, batch) = engine_rows(&shown)?;
    out.rows(Prepared::from_batches(schema, vec![batch])?.read()?).await
}
