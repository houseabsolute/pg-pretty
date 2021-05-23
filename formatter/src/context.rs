use strum_macros::AsRefStr;

#[derive(AsRefStr, Debug, PartialEq)]
pub enum ContextType {
    AExpr(u8),
}

#[macro_export]
macro_rules! new_context {
    ( $self:ident, $context:expr ) => {{
        $self.contexts.push($context);
        scopeguard::guard($self, |s| {
            s.contexts.pop();
        })
    }};
}
