"use client"
import React from "react";
import DatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";
import "./styles/DateTimePicker.css";

interface DateTimePickerProps {
  value: Date | null;
  onChange: (date: Date | null) => void;
}

const DateTimePicker: React.FC<DateTimePickerProps> = ({ value, onChange }) => {
  return (
    <div className="datetime-picker-wrapper">
      <DatePicker
        selected={value}
        onChange={onChange}
        showTimeSelect
        timeFormat="HH:mm"
        timeIntervals={1}
        dateFormat="yyyy-MM-dd HH:mm"
        minDate={new Date()}
        minTime={
          value && value.toDateString() === new Date().toDateString()
            ? new Date()
            : new Date(0, 0, 0, 0, 0)
        }
        maxTime={new Date(0, 0, 0, 23, 59)}
        placeholderText="Selelct your start time"
        onKeyDown={(e) => e.preventDefault()}
        isClearable
        className="melo-input"
        popperClassName="custom-datepicker-popper"
      />
    </div>
  );
};

DateTimePicker.displayName = "DateTimePicker";
export default React.memo(DateTimePicker);
