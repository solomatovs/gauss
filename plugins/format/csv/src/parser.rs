use server_api::PluginError;

// ═══════════════════════════════════════════════════════════════
//  Column mapping
// ═══════════════════════════════════════════════════════════════

pub(crate) struct ColumnMap {
    pub names: Vec<String>,
}

impl ColumnMap {
    /// Определить маппинг из строки заголовка.
    pub fn from_header(fields: &[String]) -> Self {
        let names: Vec<String> = fields.iter().map(|f| f.trim().to_string()).collect();
        Self { names }
    }

    /// Маппинг по умолчанию: col0, col1, col2, ...
    pub fn default_for(num_fields: usize) -> Self {
        let names: Vec<String> = (0..num_fields).map(|i| format!("col{i}")).collect();
        Self { names }
    }
}

// ═══════════════════════════════════════════════════════════════
//  RFC 4180 field parser
// ═══════════════════════════════════════════════════════════════

/// Разбирает одну RFC 4180 строку на поля с учётом quoting.
pub(crate) fn parse_fields(line: &str, delimiter: char, quoting: bool) -> Vec<String> {
    if !quoting {
        return line.split(delimiter).map(|s| s.to_string()).collect();
    }

    let mut fields = Vec::new();
    let mut chars = line.chars().peekable();
    let mut field = String::new();

    loop {
        if chars.peek() == Some(&'"') {
            // Quoted field (RFC 4180 rule 5-7)
            chars.next(); // consume opening quote
            loop {
                match chars.next() {
                    Some('"') => {
                        if chars.peek() == Some(&'"') {
                            // Escaped quote: "" → "
                            chars.next();
                            field.push('"');
                        } else {
                            // End of quoted field
                            break;
                        }
                    }
                    Some(c) => field.push(c),
                    None => break, // EOF inside quote — best effort
                }
            }
            // Consume until delimiter or end
            loop {
                match chars.peek() {
                    Some(&c) if c == delimiter => {
                        chars.next();
                        break;
                    }
                    Some(_) => { chars.next(); } // skip trailing chars after closing quote
                    None => break,
                }
            }
        } else {
            // Unquoted field
            loop {
                match chars.peek() {
                    Some(&c) if c == delimiter => {
                        chars.next();
                        break;
                    }
                    Some(_) => field.push(chars.next().unwrap()),
                    None => break,
                }
            }
        }

        fields.push(std::mem::take(&mut field));

        if chars.peek().is_none() && !line.ends_with(delimiter) {
            break;
        }
        if chars.peek().is_none() {
            // Trailing delimiter → one more empty field
            fields.push(String::new());
            break;
        }
    }

    fields
}

// ═══════════════════════════════════════════════════════════════
//  Shared config helper
// ═══════════════════════════════════════════════════════════════

pub(crate) fn parse_delimiter(s: &str) -> Result<char, PluginError> {
    match s {
        "\\t" | "\t" => Ok('\t'),
        "\\n" => Ok('\n'),
        s if s.len() == 1 => Ok(s.chars().next().unwrap()),
        other => Err(PluginError::config(format!(
            "CSV: delimiter must be a single character, got {other:?}"
        ))),
    }
}
