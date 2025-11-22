import { Form } from "react-bootstrap";

interface ValidatedCheckboxProps {
  name: string;
  label: any;
  checked: boolean;
  onChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
  isInvalid?: boolean;
  feedback?: string;
}

export default function ValidatedCheckbox({
  name,
  label,
  checked,
  onChange,
  isInvalid = false,
  feedback,
}: ValidatedCheckboxProps) {
  return (
    <Form.Group className="mb-3">
      <div className="form-check">
        <input
          type="checkbox"
          className="form-check-input" 
          id={name}
          name={name}
          checked={checked}
          onChange={onChange}
        />
        <label htmlFor={name} className="form-check-label melo-check-label">
          {label}
        </label>


        {isInvalid && feedback && (
          <div className="melo-invalid-feedback">{feedback}</div>
        )}
      </div>
    </Form.Group>
  );
}
