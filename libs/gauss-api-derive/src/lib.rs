use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, LitStr, Type};

/// Derive macro for plugin config parameter declarations.
///
/// Generates two methods on the annotated struct:
///
/// - `config_params() -> Vec<ConfigParam>` — parameter declarations for FFI export.
/// - `from_config(&ConfigValues) -> Result<Self, PluginError>` — reads typed values.
///
/// The struct must implement `Default` (defaults are used for non-required params).
///
/// # Example
///
/// ```ignore
/// #[derive(ConfigParams, Default)]
/// pub struct MyConfig {
///     #[param(context = "postmaster", description = "Buffer size")]
///     pub size: u64,
///
///     #[param(context = "sighup", description = "Mode")]
///     pub mode: String,
/// }
/// ```
///
/// Supported field types: `bool`, `i64`, `u64`, `usize`, `f64`, `String`.
#[proc_macro_derive(ConfigParams, attributes(param))]
pub fn derive_config_params(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match derive_impl(&input) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}

fn derive_impl(input: &DeriveInput) -> Result<TokenStream, syn::Error> {
    let name = &input.ident;

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    name,
                    "ConfigParams only supports structs with named fields",
                ))
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                name,
                "ConfigParams only supports structs",
            ))
        }
    };

    let mut config_param_tokens = Vec::new();
    let mut from_config_tokens = Vec::new();

    for field in fields {
        let field_name = field.ident.as_ref().ok_or_else(|| {
            syn::Error::new_spanned(field, "expected named field")
        })?;
        let field_name_str = field_name.to_string();
        let field_ty = &field.ty;

        // Parse #[param(...)] attribute.
        let mut context_str: Option<String> = None;
        let mut description_str: Option<String> = None;
        let mut required = false;

        for attr in &field.attrs {
            if !attr.path().is_ident("param") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("context") {
                    let value: LitStr = meta.value()?.parse()?;
                    context_str = Some(value.value());
                } else if meta.path.is_ident("description") {
                    let value: LitStr = meta.value()?.parse()?;
                    description_str = Some(value.value());
                } else if meta.path.is_ident("required") {
                    required = true;
                }
                Ok(())
            })?;
        }

        let context_str = context_str.ok_or_else(|| {
            syn::Error::new_spanned(field_name, "missing #[param(context = \"...\")]")
        })?;
        let description_str = description_str.ok_or_else(|| {
            syn::Error::new_spanned(field_name, "missing #[param(description = \"...\")]")
        })?;

        let context_expr = match context_str.as_str() {
            "postmaster" => quote! { gauss_api::config::ParamContext::Postmaster },
            "sighup" => quote! { gauss_api::config::ParamContext::Sighup },
            _ => {
                return Err(syn::Error::new_spanned(
                    field_name,
                    format!("unknown context '{context_str}' (expected 'postmaster' or 'sighup')"),
                ))
            }
        };

        let ty_name = type_ident_name(field_ty).ok_or_else(|| {
            syn::Error::new_spanned(field_ty, "unsupported type for ConfigParams")
        })?;

        let (param_type_expr, default_expr, getter_expr) = match ty_name.as_str() {
            "u64" => (
                quote! { gauss_api::config::ParamType::U64 },
                quote! { Some(gauss_api::config::ParamValue::U64(__defaults.#field_name)) },
                if required {
                    quote! {
                        result.#field_name = __config.get_u64(#field_name_str)
                            .ok_or_else(|| gauss_api::error::PluginError::config(
                                format!("missing required parameter '{}'", #field_name_str)
                            ))?;
                    }
                } else {
                    quote! {
                        if let Some(v) = __config.get_u64(#field_name_str) {
                            result.#field_name = v;
                        }
                    }
                },
            ),
            "usize" => (
                quote! { gauss_api::config::ParamType::U64 },
                quote! { Some(gauss_api::config::ParamValue::U64(__defaults.#field_name as u64)) },
                if required {
                    quote! {
                        result.#field_name = __config.get_u64(#field_name_str)
                            .ok_or_else(|| gauss_api::error::PluginError::config(
                                format!("missing required parameter '{}'", #field_name_str)
                            ))? as usize;
                    }
                } else {
                    quote! {
                        if let Some(v) = __config.get_u64(#field_name_str) {
                            result.#field_name = v as usize;
                        }
                    }
                },
            ),
            "i64" => (
                quote! { gauss_api::config::ParamType::I64 },
                quote! { Some(gauss_api::config::ParamValue::I64(__defaults.#field_name)) },
                if required {
                    quote! {
                        result.#field_name = __config.get_i64(#field_name_str)
                            .ok_or_else(|| gauss_api::error::PluginError::config(
                                format!("missing required parameter '{}'", #field_name_str)
                            ))?;
                    }
                } else {
                    quote! {
                        if let Some(v) = __config.get_i64(#field_name_str) {
                            result.#field_name = v;
                        }
                    }
                },
            ),
            "f64" => (
                quote! { gauss_api::config::ParamType::F64 },
                quote! { Some(gauss_api::config::ParamValue::F64(__defaults.#field_name)) },
                if required {
                    quote! {
                        result.#field_name = __config.get_f64(#field_name_str)
                            .ok_or_else(|| gauss_api::error::PluginError::config(
                                format!("missing required parameter '{}'", #field_name_str)
                            ))?;
                    }
                } else {
                    quote! {
                        if let Some(v) = __config.get_f64(#field_name_str) {
                            result.#field_name = v;
                        }
                    }
                },
            ),
            "bool" => (
                quote! { gauss_api::config::ParamType::Bool },
                quote! { Some(gauss_api::config::ParamValue::Bool(__defaults.#field_name)) },
                if required {
                    quote! {
                        result.#field_name = __config.get_bool(#field_name_str)
                            .ok_or_else(|| gauss_api::error::PluginError::config(
                                format!("missing required parameter '{}'", #field_name_str)
                            ))?;
                    }
                } else {
                    quote! {
                        if let Some(v) = __config.get_bool(#field_name_str) {
                            result.#field_name = v;
                        }
                    }
                },
            ),
            "String" => (
                quote! { gauss_api::config::ParamType::Str },
                quote! { Some(gauss_api::config::ParamValue::Str(__defaults.#field_name.clone())) },
                if required {
                    quote! {
                        result.#field_name = __config.get_str(#field_name_str)
                            .ok_or_else(|| gauss_api::error::PluginError::config(
                                format!("missing required parameter '{}'", #field_name_str)
                            ))?
                            .to_string();
                    }
                } else {
                    quote! {
                        if let Some(v) = __config.get_str(#field_name_str) {
                            result.#field_name = v.to_string();
                        }
                    }
                },
            ),
            _ => {
                return Err(syn::Error::new_spanned(
                    field_ty,
                    format!(
                        "unsupported type '{ty_name}' (expected u64, i64, f64, bool, String, usize)"
                    ),
                ))
            }
        };

        let default_value = if required {
            quote! { None }
        } else {
            default_expr
        };

        config_param_tokens.push(quote! {
            gauss_api::config::ConfigParam {
                name: #field_name_str.to_string(),
                param_type: #param_type_expr,
                context: #context_expr,
                required: #required,
                default: #default_value,
                description: #description_str.to_string(),
            }
        });

        from_config_tokens.push(getter_expr);
    }

    let expanded = quote! {
        impl #name {
            pub fn config_params() -> Vec<gauss_api::config::ConfigParam> {
                let __defaults = Self::default();
                vec![
                    #(#config_param_tokens),*
                ]
            }

            pub fn from_config(
                __config: &gauss_api::config::ConfigValues,
            ) -> Result<Self, gauss_api::error::PluginError> {
                let mut result = Self::default();
                #(#from_config_tokens)*
                Ok(result)
            }
        }
    };

    Ok(TokenStream::from(expanded))
}

/// Extract the last path segment ident name from a type (e.g. `u64`, `String`).
fn type_ident_name(ty: &Type) -> Option<String> {
    if let Type::Path(type_path) = ty {
        type_path
            .path
            .segments
            .last()
            .map(|seg| seg.ident.to_string())
    } else {
        None
    }
}
