"use client";

import { useState, useEffect, useImperativeHandle, forwardRef } from "react";
import { Form, InputGroup } from "react-bootstrap";

export interface ValidatedInputRef {
  reset: () => void;
  validateNow: () => Promise<boolean>;
}

interface ValidatedInputProps {
  label?: string;
  placeholder?: string;
  regex?: RegExp;
  validator?: (value: string) => boolean | Promise<boolean>;
  errorMessage?: string;
  successMessage?: string;
  delay?: number;
  onValidChange?: (valid: boolean, value: string) => void;
  defaultValue?: string;
  required?: boolean;
  type?: string;
}

const ValidatedInput = forwardRef<ValidatedInputRef, ValidatedInputProps>(
  (
    {
      label,
      placeholder,
      regex,
      validator,
      errorMessage = "Invalid input",
      successMessage = "Looks good!",
      delay = 500,
      onValidChange,
      defaultValue = "",
      required = false,
      type = "text",
    },
    ref
  ) => {
    const [inputValue, setInputValue] = useState(defaultValue);
    const [isValid, setIsValid] = useState<boolean | null>(null);
    const [touched, setTouched] = useState(false);

    const validate = async (val = inputValue.trim()) => {
      if (required && !val) {
        setIsValid(false);
        onValidChange?.(false, val);
        return false;
      }

      if (val === "") {
        setIsValid(null);
        onValidChange?.(false, val);
        return false;
      }

      let valid = false;

      if (validator) {
        try {
          valid = await validator(val);
        } catch {
          valid = false;
        }
      } else if (regex) {
        valid = regex.test(val);
      } else {
        console.warn("⚠️ ValidatedInput: No regex or validator provided.");
      }

      setIsValid(valid);
      onValidChange?.(valid, val);
      return valid;
    };

    useEffect(() => {
      if (!touched) return;
      const timer = setTimeout(() => validate(), delay);
      return () => clearTimeout(timer);
      // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [inputValue]);


    useImperativeHandle(ref, () => ({
      reset: () => {
        setInputValue(defaultValue);
        setIsValid(null);
        setTouched(false);
      },
      validateNow: async () => await validate(),
    }));

    return (
      <Form.Group className="mb-3" controlId={label?.toLowerCase().replace(/\s+/g, "-")}>
        {label && <Form.Label>{label}</Form.Label>}
        <InputGroup hasValidation>
          <Form.Control
            type={type}
            placeholder={placeholder}
            value={inputValue}
            required={required}
            onChange={(e) => {
              setTouched(true);
              setInputValue(e.target.value);
            }}
            isValid={isValid === true}
            isInvalid={isValid === false}
          />
          <Form.Control.Feedback type="valid">✅ {successMessage}</Form.Control.Feedback>
          <Form.Control.Feedback type="invalid">
            ❌ {required && !inputValue.trim() ? "This field is required" : errorMessage}
          </Form.Control.Feedback>
        </InputGroup>
      </Form.Group>
    );
  }
);

ValidatedInput.displayName = "ValidatedInput";
export default ValidatedInput;
